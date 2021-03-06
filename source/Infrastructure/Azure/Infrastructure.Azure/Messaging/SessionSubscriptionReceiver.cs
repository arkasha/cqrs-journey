﻿// ==============================================================================================================
// Microsoft patterns & practices
// CQRS Journey project
// ==============================================================================================================
// ©2012 Microsoft. All rights reserved. Certain content used with permission from contributors
// http://go.microsoft.com/fwlink/p/?LinkID=258575
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance 
// with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License is 
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
// See the License for the specific language governing permissions and limitations under the License.
// ==============================================================================================================

// Based on http://windowsazurecat.com/2011/09/best-practices-leveraging-windows-azure-service-bus-brokered-messaging-api/

namespace Infrastructure.Azure.Messaging
{
    using System;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Infrastructure.Azure.Instrumentation;
    using Infrastructure.Azure.Utils;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Implements an asynchronous receiver of messages from a Windows Azure 
    /// Service Bus topic subscription using sessions.
    /// </summary>
    /// <remarks>
    /// <para>
    /// In V3 we made a lot of changes to optimize the performance and scalability of the receiver.
    /// See <see href="http://go.microsoft.com/fwlink/p/?LinkID=258557"> Journey chapter 7</see> for more information on the optimizations and migration to V3.
    /// </para>
    /// <para>
    /// The current implementation uses async calls to communicate with the service bus, although the message processing is done with a blocking synchronous call.
    /// We could still make several performance improvements. For example, we could take advantage of sessions and batch multiple messages to avoid accessing the
    /// repositories multiple times where appropriate. See <see href="http://go.microsoft.com/fwlink/p/?LinkID=258557"> Journey chapter 7</see> for more potential 
    /// performance and scalability optimizations.
    /// </para>
    /// </remarks>
    public class SessionSubscriptionReceiver : IMessageReceiver, IDisposable
    {
        private static readonly TimeSpan AcceptSessionLongPollingTimeout = TimeSpan.FromMinutes(1);

        private readonly TokenProvider tokenProvider;
        private readonly Uri serviceUri;
        private readonly ServiceBusSettings settings;
        private readonly string topic;
        private readonly string subscription;
        private readonly bool requiresSequentialProcessing;
        private readonly object lockObject = new object();
        private readonly Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.RetryPolicy receiveRetryPolicy;
        private readonly ISessionSubscriptionReceiverInstrumentation instrumentation;
        private readonly DynamicThrottling dynamicThrottling;
        private CancellationTokenSource cancellationSource;
        private SubscriptionClient client;

        /// <summary>
        /// Initializes a new instance of the <see cref="SubscriptionReceiver"/> class, 
        /// automatically creating the topic and subscription if they don't exist.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "Instrumentation disabled in this overload")]
        public SessionSubscriptionReceiver(ServiceBusSettings settings, string topic, string subscription, bool requiresSequentialProcessing = true)
            : this(
                settings,
                topic,
                subscription,
                requiresSequentialProcessing,
                new SessionSubscriptionReceiverInstrumentation(subscription, false),
                new ExponentialBackoff(10, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(1)))
        {
        }

        public SessionSubscriptionReceiver(ServiceBusSettings settings, string topic, string subscription, bool requiresSequentialProcessing, ISessionSubscriptionReceiverInstrumentation instrumentation)
            : this(
                settings,
                topic,
                subscription,
                requiresSequentialProcessing,
                instrumentation,
                new ExponentialBackoff(10, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(1)))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SessionSubscriptionReceiver"/> class, 
        /// automatically creating the topic and subscription if they don't exist.
        /// </summary>
        protected SessionSubscriptionReceiver(ServiceBusSettings settings, string topic, string subscription, bool requiresSequentialProcessing, ISessionSubscriptionReceiverInstrumentation instrumentation, RetryStrategy backgroundRetryStrategy)
        {
            this.settings = settings;
            this.topic = topic;
            this.subscription = subscription;
            this.requiresSequentialProcessing = requiresSequentialProcessing;
            this.instrumentation = instrumentation;

            var messagingFactory = MessagingFactory.CreateFromConnectionString(settings.ConnectionString);
            this.client = messagingFactory.CreateSubscriptionClient(topic, subscription);
            if (this.requiresSequentialProcessing)
            {
                this.client.PrefetchCount = 10;
            }
            else
            {
                this.client.PrefetchCount = 15;
            }

            this.dynamicThrottling =
                new DynamicThrottling(
                    maxDegreeOfParallelism: 160,
                    minDegreeOfParallelism: 30,
                    penaltyAmount: 3,
                    workFailedPenaltyAmount: 5,
                    workCompletedParallelismGain: 1,
                    intervalForRestoringDegreeOfParallelism: 10000);
            this.receiveRetryPolicy = new RetryPolicy<ServiceBusTransientErrorDetectionStrategy>(backgroundRetryStrategy);
            this.receiveRetryPolicy.Retrying += (s, e) =>
                {
                    this.dynamicThrottling.Penalize();
                    Trace.TraceWarning(
                        "An error occurred in attempt number {1} to receive a message from subscription {2}: {0}",
                        e.LastException.Message,
                        e.CurrentRetryCount,
                        this.subscription);
                };
        }

        /// <summary>
        /// Handler for incoming messages. The return value indicates whether the message should be disposed.
        /// </summary>
        protected Func<BrokeredMessage, MessageReleaseAction> MessageHandler { get; private set; }

        /// <summary>
        /// Starts the listener.
        /// </summary>
        public void Start(Func<BrokeredMessage, MessageReleaseAction> messageHandler)
        {
            lock (this.lockObject)
            {
                // If it's not null, there is already a listening task.
                if (this.cancellationSource == null)
                {
                    this.MessageHandler = messageHandler;
                    this.cancellationSource = new CancellationTokenSource();
                    Task.Factory.StartNew(() => this.AcceptSessionAsync(this.cancellationSource.Token), this.cancellationSource.Token);
                    this.dynamicThrottling.Start(this.cancellationSource.Token);
                }
            }
        }

        /// <summary>
        /// Stops the listener.
        /// </summary>
        public void Stop()
        {
            lock (this.lockObject)
            {
                using (this.cancellationSource)
                {
                    if (this.cancellationSource != null)
                    {
                        this.cancellationSource.Cancel();
                        this.cancellationSource = null;
                        this.MessageHandler = null;
                    }
                }
            }
        }

        /// <summary>
        /// Stops the listener if it was started previously.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            this.Stop();

            if (disposing)
            {
                using (this.instrumentation as IDisposable) { }
                using (this.dynamicThrottling as IDisposable) { }
            }
        }

        protected virtual MessageReleaseAction InvokeMessageHandler(BrokeredMessage message)
        {
            return this.MessageHandler != null ? this.MessageHandler(message) : MessageReleaseAction.AbandonMessage;
        }

        ~SessionSubscriptionReceiver()
        {
            this.Dispose(false);
        }

        private async Task AcceptSessionAsync(CancellationToken cancellationToken)
        {
            this.dynamicThrottling.WaitUntilAllowedParallelism(cancellationToken);

            if (!cancellationToken.IsCancellationRequested)
            {

                try
                {
                    var session = await this.receiveRetryPolicy.ExecuteAsync(() => this.client.AcceptMessageSessionAsync(AcceptSessionLongPollingTimeout), cancellationToken);
                    if (session != null)
                    {
                        this.instrumentation.SessionStarted();
                        this.dynamicThrottling.NotifyWorkStarted();
                        // starts a new task to process new sessions in parallel when enough threads are available
                        await Task.Factory.StartNew(async () => await this.AcceptSessionAsync(cancellationToken), cancellationToken);
                        this.ReceiveMessagesAndCloseSession(session, cancellationToken);
                    }
                    else
                    {
                        await this.AcceptSessionAsync(cancellationToken);
                    }
                }
                catch (AggregateException ae)
                {
                    // Just log an exception. Do not allow an unhandled exception to terminate the message receive loop abnormally.
                    Trace.TraceError("An unrecoverable error occurred while trying to accept a session in subscription {1}:\r\n{0}", ae.InnerException, this.subscription);
                    this.dynamicThrottling.Penalize();

                    if (!cancellationToken.IsCancellationRequested)
                    {
                        // Continue accepting new sessions until told to stop regardless of any exceptions.
                        await Task.Delay(10000, cancellationToken);
                    }
                    throw;
                }
            }
        }

        /// <summary>
        /// Receives the messages in an asynchronous loop and closes the session once there are no more messages.
        /// </summary>
        private void ReceiveMessagesAndCloseSession(MessageSession session, CancellationToken cancellationToken)
        {
            CountdownEvent unreleasedMessages = new CountdownEvent(1);

            Action<bool> closeSession = (bool success) =>
            {
                Action doClose = () =>
                    {
                        try
                        {
                            unreleasedMessages.Signal();
                            if (!unreleasedMessages.Wait(15000, cancellationToken))
                            {
                                Trace.TraceWarning("Waited for pending unreleased messages before closing session in subscription {0} but they did not complete in time", this.subscription);
                            }
                        }
                        catch (OperationCanceledException)
                        {
                        }
                        finally
                        {
                            unreleasedMessages.Dispose();
                        }
                        this.receiveRetryPolicy.ExecuteAsync(session.CloseAsync, cancellationToken).ContinueWith(t =>
                        {
                            if (t.IsFaulted)
                            {
                                this.instrumentation.SessionEnded();
                                Trace.TraceError(
                                    "An unrecoverable error occurred while trying to close a session in subscription {1}:\r\n{0}",
                                    t.Exception.InnerException, this.subscription);
                                this.dynamicThrottling.NotifyWorkCompletedWithError();
                            }
                            else
                            {
                                this.instrumentation.SessionEnded();
                                if (success)
                                {
                                    this.dynamicThrottling.NotifyWorkCompleted();
                                }
                                else
                                {
                                    this.dynamicThrottling.NotifyWorkCompletedWithError();
                                }
                            }
                        });
                    };

                if (this.requiresSequentialProcessing)
                {
                    doClose.Invoke();
                }
                else
                {
                    // Allow some time for releasing the messages before closing. Also, continue in a non I/O completion thread in order to block.
                    Task.Delay(200).ContinueWith(t => doClose());
                }
            };

            // Declare an action to receive the next message in the queue or closes the session if cancelled.
            Action receiveNext = null;

            // Declare an action acting as a callback whenever a non-transient exception occurs while receiving or processing messages.
            Action<Exception> recoverReceive = null;

            // Declare an action responsible for the core operations in the message receive loop.
            Action receiveMessage = (() =>
            {
                // Use a retry policy to execute the Receive action in an asynchronous and reliable fashion.
                this.receiveRetryPolicy.ExecuteAsync(() => session.ReceiveAsync(TimeSpan.Zero), cancellationToken).ContinueWith(t =>
                {
                    if (!t.IsFaulted && !t.IsCanceled)
                    {
                        var msg = t.Result;

                        // Process the message once it was successfully received
                        // Check if we actually received any messages.
                        if (msg != null)
                        {
                            var roundtripStopwatch = Stopwatch.StartNew();
                            long schedulingElapsedMilliseconds = 0;
                            long processingElapsedMilliseconds = 0;

                            unreleasedMessages.AddCount();

                            Task.Factory.StartNew(async () =>
                            {
                                var releaseAction = MessageReleaseAction.AbandonMessage;

                                try
                                {
                                    this.instrumentation.MessageReceived();

                                    schedulingElapsedMilliseconds = roundtripStopwatch.ElapsedMilliseconds;

                                    // Make sure the process was told to stop receiving while it was waiting for a new message.
                                    if (!cancellationToken.IsCancellationRequested)
                                    {
                                        try
                                        {
                                            try
                                            {
                                                // Process the received message.
                                                releaseAction = this.InvokeMessageHandler(msg);

                                                processingElapsedMilliseconds = roundtripStopwatch.ElapsedMilliseconds - schedulingElapsedMilliseconds;
                                                this.instrumentation.MessageProcessed(releaseAction.Kind == MessageReleaseActionKind.Complete, processingElapsedMilliseconds);
                                            }
                                            catch
                                            {
                                                processingElapsedMilliseconds = roundtripStopwatch.ElapsedMilliseconds - schedulingElapsedMilliseconds;
                                                this.instrumentation.MessageProcessed(false, processingElapsedMilliseconds);

                                                throw;
                                            }
                                        }
                                        finally
                                        {
                                            if (roundtripStopwatch.Elapsed > TimeSpan.FromSeconds(45))
                                            {
                                                this.dynamicThrottling.Penalize();
                                            }
                                        }
                                    }
                                }
                                finally
                                {
                                    // Ensure that any resources allocated by a BrokeredMessage instance are released.
                                    if (this.requiresSequentialProcessing)
                                    {
                                        await this.ReleaseMessageAsync(msg, releaseAction, () => { receiveNext(); }, () => { closeSession(false); }, unreleasedMessages, processingElapsedMilliseconds, schedulingElapsedMilliseconds, roundtripStopwatch);
                                    }
                                    else
                                    {
                                        // Receives next without waiting for the message to be released.
                                        await this.ReleaseMessageAsync(msg, releaseAction, () => { }, () => { this.dynamicThrottling.Penalize(); }, unreleasedMessages, processingElapsedMilliseconds, schedulingElapsedMilliseconds, roundtripStopwatch);
                                        receiveNext.Invoke();
                                    }
                                }
                            });
                        }
                        else
                        {
                            // no more messages in the session, close it and do not continue receiving
                            closeSession(true);
                        }
                    }
                    else if (t.IsFaulted)
                    {
                        // Invoke a custom action to indicate that we have encountered an exception and
                        // need further decision as to whether to continue receiving messages.
                        recoverReceive.Invoke(t.Exception);
                    }
                });
            });

            // Initialize an action to receive the next message in the queue or closes the session if cancelled.
            receiveNext = () =>
            {
                if (!cancellationToken.IsCancellationRequested)
                {
                    // Continue receiving and processing new messages until told to stop.
                    receiveMessage.Invoke();
                }
                else
                {
                    closeSession(true);
                }
            };

            // Initialize a custom action acting as a callback whenever a non-transient exception occurs while receiving or processing messages.
            recoverReceive = ex =>
            {
                // Just log an exception. Do not allow an unhandled exception to terminate the message receive loop abnormally.
                if (ex is AggregateException)
                {
                    var innerExceptions = string.Join("\r\n", ((AggregateException)ex).Flatten().InnerExceptions.Select(e => e.ToString()));
                    
                    Trace.TraceError("Unrecoverable errors occurred while trying to receive a new message from subscription {1}:\r\n{0}", innerExceptions, this.subscription);
                }
                else
                {
                    Trace.TraceError("An unrecoverable error occurred while trying to receive a new message from subscription {1}:\r\n{0}", ex, this.subscription);
                }

                // Cannot continue to receive messages from this session.
                closeSession(false);
            };

            // Start receiving messages asynchronously for the session.
            receiveNext.Invoke();
        }

        private async Task ReleaseMessageAsync(BrokeredMessage msg, MessageReleaseAction releaseAction, Action completeReceive, Action onReleaseError, CountdownEvent countdown, long processingElapsedMilliseconds, long schedulingElapsedMilliseconds, Stopwatch roundtripStopwatch)
        {
            bool operationSucceeded = false;

            switch (releaseAction.Kind)
            {
                case MessageReleaseActionKind.Complete:
                    operationSucceeded = await msg.SafeCompleteAsync(
                        this.subscription);
                    break;
                case MessageReleaseActionKind.Abandon:
                    operationSucceeded = await msg.SafeAbandonAsync(
                        this.subscription);
                    break;
                case MessageReleaseActionKind.DeadLetter:
                    operationSucceeded = await msg.SafeDeadLetterAsync(
                        this.subscription);
                    break;
                default:
                    break;
            }

            msg.Dispose();
            this.OnMessageCompleted(false, countdown);

            if (operationSucceeded)
            {
                completeReceive();
            }
            else
            {
                onReleaseError();
            }
        }

        private void OnMessageCompleted(bool success, CountdownEvent countdown)
        {
            this.instrumentation.MessageCompleted(success);
            try
            {
                countdown.Signal();
            }
            catch (ObjectDisposedException)
            {
                // It could happen in a rare case that due to a timing issue between closing the session and disposing the countdown,
                // that the countdown is already disposed. This is OK and it can continue processing normally.
            }
        }
    }
}

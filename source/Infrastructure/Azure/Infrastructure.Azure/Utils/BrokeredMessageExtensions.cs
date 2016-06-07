// ==============================================================================================================
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

namespace Infrastructure.Azure.Utils
{
    using System;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.ServiceBus.Messaging;

    public static class BrokeredMessageExtensions
    {
        private static readonly RetryStrategy retryStrategy =
            new ExponentialBackoff(3, TimeSpan.FromSeconds(.5d), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2)) { FastFirstRetry = true };

        public static async Task<bool> SafeCompleteAsync(this BrokeredMessage message, string subscription)
        {
            return await SafeMessagingActionAsync(
                message.CompleteAsync,
                message,
                "An error occurred while completing message {0} in subscription {1} with processing time {3} (scheduling {4} request {5} roundtrip {6}). Error message: {2}",
                message.MessageId,
                subscription);
        }

        public static async Task<bool> SafeAbandonAsync(this BrokeredMessage message, string subscription)
        {
            return await SafeMessagingActionAsync(
                message.AbandonAsync,
                message,
                "An error occurred while abandoning message {0} in subscription {1} with processing time {3} (scheduling {4} request {5} roundtrip {6}). Error message: {2}",
                message.MessageId,
                subscription);
        }

        public static async Task<bool> SafeDeadLetterAsync(this BrokeredMessage message, string subscription)
        {
            return await SafeMessagingActionAsync(
                message.DeadLetterAsync,
                message,
                "An error occurred while dead-lettering message {0} in subscription {1} with processing time {3} (scheduling {4} request {5} roundtrip {6}). Error message: {2}",
                message.MessageId,
                subscription);
        }

        internal static async Task<bool> SafeMessagingActionAsync(Func<Task> task, BrokeredMessage message, string actionErrorDescription, string messageId, string subscription)
        {
            var retryPolicy = new RetryPolicy<ServiceBusTransientErrorDetectionStrategy>(retryStrategy);
            retryPolicy.Retrying +=
                (s, e) =>
                {
                    Trace.TraceWarning("An error occurred in attempt number {1} to release message {3} in subscription {2}: {0}",
                    e.LastException.GetType().Name + " - " + e.LastException.Message,
                    e.CurrentRetryCount,
                    subscription,
                    message.MessageId);
                };

            try
            {
                await retryPolicy.ExecuteAsync(task);

                return true;
            }
            catch (Exception ex)
            {
                if (ex is MessageLockLostException || ex is MessagingException || ex is TimeoutException)
                {
                    Trace.TraceWarning(actionErrorDescription, messageId, subscription, ex.GetType().Name + " - " + ex.Message);
                }
                else
                {
                    Trace.TraceError("Unexpected error releasing message in subscription {1}:\r\n{0}", ex, subscription);
                }

                return false;
            }
        }
    }
}

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

namespace Infrastructure.Azure.IntegrationTests.TopicSenderIntegration
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Infrastructure.Azure.Messaging;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Xunit;

    public class given_a_topic_sender : given_a_topic_and_subscription
    {
        private SubscriptionClient subscriptionClient;
        private TestableTopicSender sut;

        public given_a_topic_sender()
        {
            this.sut = new TestableTopicSender(this.Settings, this.Topic, new Incremental(1, TimeSpan.Zero, TimeSpan.Zero));

            var manager = NamespaceManager.CreateFromConnectionString(this.Settings.ConnectionString);
            manager.CreateSubscription(this.Topic, "Test");

            var messagingFactory = MessagingFactory.CreateFromConnectionString(this.Settings.ConnectionString);
            this.subscriptionClient = messagingFactory.CreateSubscriptionClient(this.Topic, "Test");

        }

        [Fact]
        public void when_sending_message_async_then_succeeds()
        {
            var payload = Guid.NewGuid().ToString();

            this.sut.SendAsync(() => new BrokeredMessage(payload));

            var message = this.subscriptionClient.Receive(TimeSpan.FromSeconds(5));
            Assert.Equal(payload, message.GetBody<string>());
        }

        [Fact]
        public void when_sending_message_batch_async_then_succeeds()
        {
            var payload1 = Guid.NewGuid().ToString();
            var payload2 = Guid.NewGuid().ToString();

            this.sut.SendAsync(new Func<BrokeredMessage>[] { () => new BrokeredMessage(payload1), () => new BrokeredMessage(payload2) });

            var messages = new List<string>
                               {
                                   this.subscriptionClient.Receive(TimeSpan.FromSeconds(5)).GetBody<string>(),
                                   this.subscriptionClient.Receive(TimeSpan.FromSeconds(2)).GetBody<string>()
                               };
            Assert.Contains(payload1, messages);
            Assert.Contains(payload2, messages);
        }

        [Fact]
        public void when_sending_message_then_succeeds()
        {
            var payload = Guid.NewGuid().ToString();
            this.sut.Send(() => new BrokeredMessage(payload));

            var message = this.subscriptionClient.Receive();
            Assert.Equal(payload, message.GetBody<string>());
        }

        [Fact]
        public void when_sending_message_fails_transiently_once_then_retries()
        {
            var payload = Guid.NewGuid().ToString();

            var attempt = 0;
            var signal = new AutoResetEvent(false);
            var currentDelegate = this.sut.DoBeginSendMessageDelegate;
            this.sut.DoBeginSendMessageDelegate = this.sut.DoBeginSendMessageDelegate =
                (mf) =>
                {
                    if (attempt++ == 0) throw new TimeoutException();
                    signal.Set();
                    return currentDelegate(mf);
                };

            this.sut.SendAsync(() => new BrokeredMessage(payload));

            var message = this.subscriptionClient.Receive(TimeSpan.FromSeconds(5));
            Assert.True(signal.WaitOne(TimeSpan.FromSeconds(5)), "Test timed out");
            Assert.Equal(payload, message.GetBody<string>());
            Assert.Equal(2, attempt);
        }

        [Fact]
        public void when_sending_message_fails_transiently_multiple_times_then_fails()
        {
            var payload = Guid.NewGuid().ToString();

            var currentDelegate = this.sut.DoBeginSendMessageDelegate;
            this.sut.DoBeginSendMessageDelegate = brokeredMessage =>
            {
                throw new TimeoutException();
            };

            this.sut.SendAsync(() => new BrokeredMessage(payload));

            var message = this.subscriptionClient.Receive(TimeSpan.FromSeconds(5));
            Assert.Null(message);
        }
    }

    public class TestableTopicSender : TopicSender
    {
        public TestableTopicSender(ServiceBusSettings settings, string topic, RetryStrategy retryStrategy)
            : base(settings, topic, retryStrategy)
        {
            this.DoBeginSendMessageDelegate = base.DoSendMessageAsync;
        }

        public Func<BrokeredMessage, Task> DoBeginSendMessageDelegate;

        protected override async Task DoSendMessageAsync(BrokeredMessage message)
        {
            await this.DoBeginSendMessageDelegate(message);
        }
    }
}

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

namespace Infrastructure.Azure.Tests.EventSourcing.EventStoreBusPublisherFixture
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Infrastructure.Azure.EventSourcing;
    using Infrastructure.Azure.Instrumentation;
    using Infrastructure.Azure.Tests.Mocks;
    using Microsoft.WindowsAzure.Storage.Table;
    using Moq;
    using Xunit;

    public class when_calling_publish
    {
        private Mock<IPendingEventsQueue> queue;
        private MessageSenderMock sender;
        private string partitionKey;
        private string version;
        private IEventRecord testEvent;

        public when_calling_publish()
        {
            this.partitionKey = Guid.NewGuid().ToString();
            this.version = "0001";
            string rowKey = "Unpublished_" + this.version;
            this.testEvent = Mock.Of<IEventRecord>(x =>
                x.PartitionKey == this.partitionKey
                && x.RowKey == rowKey
                && x.TypeName == "TestEventType"
                && x.SourceId == "TestId"
                && x.SourceType == "TestSourceType"
                && x.Payload == "serialized event"
                && x.CorrelationId == "correlation"
                && x.AssemblyName == "Assembly"
                && x.Namespace == "Namespace"
                && x.FullName == "Namespace.TestEventType");
            this.queue = new Mock<IPendingEventsQueue>();
            this.queue.Setup(x => x.GetPendingAsync(this.partitionKey))
                .Returns(Task.FromResult((new[] { this.testEvent }).AsEnumerable()));

            this.sender = new MessageSenderMock();
            var sut = new EventStoreBusPublisher(this.sender, this.queue.Object, new MockEventStoreBusPublisherInstrumentation());
            var cancellationTokenSource = new CancellationTokenSource();
            sut.Start(cancellationTokenSource.Token);

            sut.Send(this.partitionKey, 0);

            Assert.True(this.sender.SendSignal.WaitOne(int.MaxValue));
            cancellationTokenSource.Cancel();
        }

        [Fact]
        public void then_sends_unpublished_event_with_deterministic_message_id_for_detecting_duplicates()
        {
            string expectedMessageId = string.Format("{0}_{1}", this.partitionKey, this.version);
            Assert.Equal(expectedMessageId, this.sender.Sent.Single().MessageId);
        }

        [Fact]
        public void then_sent_event_contains_friendly_metadata()
        {
            Assert.Equal(this.testEvent.SourceId, this.sender.Sent.Single().Properties[StandardMetadata.SourceId]);
            Assert.Equal(this.testEvent.SourceType, this.sender.Sent.Single().Properties[StandardMetadata.SourceType]);
            Assert.Equal(this.testEvent.TypeName, this.sender.Sent.Single().Properties[StandardMetadata.TypeName]);
            Assert.Equal(this.testEvent.FullName, this.sender.Sent.Single().Properties[StandardMetadata.FullName]);
            Assert.Equal(this.testEvent.Namespace, this.sender.Sent.Single().Properties[StandardMetadata.Namespace]);
            Assert.Equal(this.testEvent.AssemblyName, this.sender.Sent.Single().Properties[StandardMetadata.AssemblyName]);
            Assert.Equal(this.version, this.sender.Sent.Single().Properties["Version"]);
            Assert.Equal(this.testEvent.CorrelationId, this.sender.Sent.Single().CorrelationId);
        }

        [Fact]
        public void then_deletes_message_after_publishing()
        {
            this.queue.Verify(q => q.DeletePendingAsync(this.partitionKey, this.testEvent.RowKey));
        }
    }

    public class when_starting_with_pending_events
    {
        private Mock<IPendingEventsQueue> queue;
        private MessageSenderMock sender;
        private string version;
        private string[] pendingKeys;
        private string rowKey;

        public when_starting_with_pending_events()
        {
            this.version = "0001";
            this.rowKey = "Unpublished_" + this.version;

            this.pendingKeys = new[] { "Key1", "Key2", "Key3" };
            this.queue = new Mock<IPendingEventsQueue>();
            this.queue.Setup(x => x.GetPendingAsync(It.IsAny<string>()))
                .Callback<string, Action<IEnumerable<IEventRecord>, TableContinuationToken>, Action<Exception>>(
                (key, success, error) =>
                    success(new[]
                           {
                               Mock.Of<IEventRecord>(
                                   x => x.PartitionKey == key
                                        && x.RowKey == this.rowKey
                                        && x.TypeName == "TestEventType"
                                        && x.SourceId == "TestId"
                                        && x.SourceType == "TestSourceType"
                                        && x.Payload == "serialized event")
                           },
                    null));


            this.queue.Setup(x => x.GetPartitionsWithPendingEvents()).Returns(Task.FromResult(this.pendingKeys.AsEnumerable()));
            this.sender = new MessageSenderMock();
            var sut = new EventStoreBusPublisher(this.sender, this.queue.Object, new MockEventStoreBusPublisherInstrumentation());
            var cancellationTokenSource = new CancellationTokenSource();
            sut.Start(cancellationTokenSource.Token);

            for (int i = 0; i < this.pendingKeys.Length; i++)
            {
                Assert.True(this.sender.SendSignal.WaitOne(5000));
            }
            cancellationTokenSource.Cancel();
        }

        [Fact]
        public void then_sends_unpublished_event_with_deterministic_message_id_for_detecting_duplicates()
        {
            for (int i = 0; i < this.pendingKeys.Length; i++)
            {
                string expectedMessageId = string.Format("{0}_{1}", this.pendingKeys[i], this.version);
                Assert.True(this.sender.Sent.Any(x => x.MessageId == expectedMessageId));
            }
        }

        [Fact]
        public void then_sent_event_contains_friendly_metadata()
        {
            for (int i = 0; i < this.pendingKeys.Length; i++)
            {
                var message = this.sender.Sent.ElementAt(i);
                Assert.Equal("TestId", message.Properties[StandardMetadata.SourceId]);
                Assert.Equal("TestSourceType", message.Properties["SourceType"]);
                Assert.Equal("TestEventType", message.Properties[StandardMetadata.TypeName]);
                Assert.Equal(this.version, message.Properties["Version"]);
            }
        }

        [Fact]
        public void then_deletes_message_after_publishing()
        {
            for (int i = 0; i < this.pendingKeys.Length; i++)
            {
                this.queue.Verify(q => q.DeletePendingAsync(this.pendingKeys[i], this.rowKey));
            }
        }
    }

    public class given_event_store_with_events_after_it_is_started : IDisposable
    {
        private Mock<IPendingEventsQueue> queue;
        private MessageSenderMock sender;
        private string version;
        private string[] partitionKeys;
        private string rowKey;
        private EventStoreBusPublisher sut;
        private CancellationTokenSource cancellationTokenSource;

        public given_event_store_with_events_after_it_is_started()
        {
            this.version = "0001";
            this.rowKey = "Unpublished_" + this.version;

            this.partitionKeys = Enumerable.Range(0, 200).Select(i => "Key" + i).ToArray();
            this.queue = new Mock<IPendingEventsQueue>();
            this.queue.Setup(x => x.GetPendingAsync(It.IsAny<string>()))
                .Callback<string, Action<IEnumerable<IEventRecord>, TableContinuationToken>, Action<Exception>>(
                (key, success, error) =>
                    success(new[]
                                {
                                    Mock.Of<IEventRecord>(
                                        x => x.PartitionKey == key
                                            && x.RowKey == this.rowKey
                                            && x.TypeName == "TestEventType"
                                            && x.SourceId == "TestId"
                                            && x.SourceType == "TestSourceType"
                                            && x.Payload == "serialized event")
                                },
                    null));

            this.queue.Setup(x => x.GetPartitionsWithPendingEvents()).Returns(Task.FromResult(Enumerable.Empty<string>()));
            this.queue
                .Setup(x =>
                    x.DeletePendingAsync(
                        It.IsAny<string>(),
                        It.IsAny<string>()))
                .Callback<string, string, Action<bool>, Action<Exception>>((p, r, s, e) => s(true));
            this.sender = new MessageSenderMock();
            this.sut = new EventStoreBusPublisher(this.sender, this.queue.Object, new MockEventStoreBusPublisherInstrumentation());
            this.cancellationTokenSource = new CancellationTokenSource();
            this.sut.Start(this.cancellationTokenSource.Token);
        }

        [Fact]
        public void when_sending_multiple_partitions_immediately_then_publishes_all_events()
        {
            for (int i = 0; i < this.partitionKeys.Length; i++)
            {
                this.sut.Send(this.partitionKeys[i], 0);
            }

            var timeout = TimeSpan.FromSeconds(20);
            var stopwatch = Stopwatch.StartNew();
            while (this.sender.Sent.Count < this.partitionKeys.Length && stopwatch.Elapsed < timeout)
            {
                Thread.Sleep(300);
            }

            Assert.Equal(this.partitionKeys.Length, this.sender.Sent.Count);
            for (int i = 0; i < this.partitionKeys.Length; i++)
            {
                string expectedMessageId = string.Format("{0}_{1}", this.partitionKeys[i], this.version);
                Assert.NotNull(this.sender.Sent.Single(x => x.MessageId == expectedMessageId));
            }
        }

        [Fact]
        public void when_send_takes_time_then_still_publishes_events_concurrently_with_throttling()
        {
            this.sender.ShouldWaitForCallback = true;
            for (int i = 0; i < this.partitionKeys.Length; i++)
            {
                this.sut.Send(this.partitionKeys[i], 0);
            }

            Thread.Sleep(1000);

            Assert.True(this.sender.Sent.Count < this.partitionKeys.Length);
            Assert.True(this.sender.AsyncSuccessCallbacks.Count < this.partitionKeys.Length);

            this.sender.ShouldWaitForCallback = false;
            foreach (var callback in this.sender.AsyncSuccessCallbacks)
            {
                callback.Invoke();
            }

            // once all events can be sent, verify that it sends all.
            var stopwatch = Stopwatch.StartNew();
            while (this.sender.Sent.Count < this.partitionKeys.Length && stopwatch.Elapsed < TimeSpan.FromSeconds(20))
            {
                Thread.Sleep(300);
            }

            Assert.Equal(this.partitionKeys.Length, this.sender.Sent.Count);
            for (int i = 0; i < this.partitionKeys.Length; i++)
            {
                string expectedMessageId = string.Format("{0}_{1}", this.partitionKeys[i], this.version);
                Assert.NotNull(this.sender.Sent.Single(x => x.MessageId == expectedMessageId));
            }
        }

        public void Dispose()
        {
            this.cancellationTokenSource.Cancel();
        }
    }

    class MockEventStoreBusPublisherInstrumentation : IEventStoreBusPublisherInstrumentation
    {
        void IEventStoreBusPublisherInstrumentation.EventsPublishingRequested(int eventCount) { }
        void IEventStoreBusPublisherInstrumentation.EventPublished() { }
        void IEventStoreBusPublisherInstrumentation.EventPublisherStarted() { }
        void IEventStoreBusPublisherInstrumentation.EventPublisherFinished() { }
    }
}

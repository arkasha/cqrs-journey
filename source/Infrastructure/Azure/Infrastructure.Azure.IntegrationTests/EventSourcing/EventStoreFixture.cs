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

namespace Infrastructure.Azure.IntegrationTests.EventSourcing.EventStoreFixture
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Globalization;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Infrastructure;
    using Infrastructure.Azure;
    using Infrastructure.Azure.EventSourcing;
    using Microsoft.WindowsAzure.Storage;
    using Xunit;

    public class given_empty_store : IDisposable
    {
        private readonly string tableName;
        private CloudStorageAccount account;
        protected EventStore sut;
        protected string sourceId;
        protected string partitionKey;
        protected EventData[] events;

        public given_empty_store()
        {
            this.tableName = "EventStoreFixture" + new Random((int)DateTime.Now.Ticks).Next();
            var settings = InfrastructureSettings.Read("Settings.xml").EventSourcing;
            this.account = CloudStorageAccount.Parse(settings.ConnectionString);
            this.sut = new EventStore(this.account, this.tableName);

            this.sourceId = Guid.NewGuid().ToString();
            this.partitionKey = Guid.NewGuid().ToString();
            this.events = new[]
                             {
                                 new EventData { Version = 1, SourceId = this.sourceId, SourceType = "Source", TypeName = "Test1", Payload = "Payload1", CorrelationId = "correlation1" },
                                 new EventData { Version = 2, SourceId = this.sourceId, SourceType = "Source", TypeName = "Test2", Payload = "Payload2", CorrelationId = "correlation2"  },
                                 new EventData { Version = 3, SourceId = this.sourceId, SourceType = "Source", TypeName = "Test3", Payload = "Payload3", CorrelationId = "correlation3"  },
                             };
        }

        public void Dispose()
        {
            var client = this.account.CreateCloudTableClient();
            var table = client.GetTableReference(this.tableName);
            table.DeleteIfExists();
        }
    }

    public class when_adding_items : given_empty_store
    {
        [Fact]
        public async Task when_adding_one_item_then_can_load_it()
        {
            await this.sut.Save(this.partitionKey, new[] { this.events[0] });

            var stored = (await this.sut.Load(this.partitionKey, 0)).ToList();

            Assert.Equal(1, stored.Count);
            Assert.Equal(this.events[0].Version, stored[0].Version);
            Assert.Equal(this.events[0].SourceId, stored[0].SourceId);
            Assert.Equal(this.events[0].TypeName, stored[0].TypeName);
            Assert.Equal(this.events[0].Payload, stored[0].Payload);
            Assert.Equal(this.events[0].CorrelationId, stored[0].CorrelationId);
        }

        [Fact]
        public async Task when_adding_multiple_items_then_can_load_them_in_order()
        {
            await this.sut.Save(this.partitionKey, this.events);

            var stored = (await this.sut.Load(this.partitionKey, 0)).ToList();

            Assert.Equal(3, stored.Count);
            Assert.True(stored.All(x => x.SourceType == "Source"));
            Assert.True(stored.All(x => x.SourceId == this.sourceId));
            Assert.Equal(1, stored[0].Version);
            Assert.Equal(2, stored[1].Version);
            Assert.Equal(3, stored[2].Version);
            Assert.Equal("Payload1", stored[0].Payload);
            Assert.Equal("Payload2", stored[1].Payload);
            Assert.Equal("Payload3", stored[2].Payload);
            Assert.Equal("correlation1", stored[0].CorrelationId);
            Assert.Equal("correlation2", stored[1].CorrelationId);
            Assert.Equal("correlation3", stored[2].CorrelationId);
        }

        [Fact]
        public async Task when_adding_multiple_items_at_different_times_then_can_load_them_in_order()
        {
            await this.sut.Save(this.partitionKey, new[] { this.events[0], this.events[1] });
            await this.sut.Save(this.partitionKey, new[] { this.events[2] });

            var stored = (await this.sut.Load(this.partitionKey, 0)).ToList();

            Assert.Equal(3, stored.Count);
            Assert.True(stored.All(x => x.SourceType == "Source"));
            Assert.Equal(1, stored[0].Version);
            Assert.Equal(2, stored[1].Version);
            Assert.Equal(3, stored[2].Version);
            Assert.Equal("Payload1", stored[0].Payload);
            Assert.Equal("Payload2", stored[1].Payload);
            Assert.Equal("Payload3", stored[2].Payload);
            Assert.Equal("correlation1", stored[0].CorrelationId);
            Assert.Equal("correlation2", stored[1].CorrelationId);
            Assert.Equal("correlation3", stored[2].CorrelationId);
        }

        [Fact]
        public async Task can_load_events_since_specified_version()
        {
            await this.sut.Save(this.partitionKey, this.events);

            var stored = (await this.sut.Load(this.partitionKey, 2)).ToList();

            Assert.Equal(2, stored.Count);
            Assert.Equal(2, stored[0].Version);
            Assert.Equal(3, stored[1].Version);
            Assert.Equal("Payload2", stored[0].Payload);
            Assert.Equal("Payload3", stored[1].Payload);
        }

        [Fact]
        public async Task cannot_store_same_version()
        {
            await this.sut.Save(this.partitionKey, new[] { this.events[0] });

            var sameVersion = new EventData { Version = this.events[0].Version, TypeName = "Test2", Payload = "Payload2" };
            await Assert.ThrowsAsync<ConcurrencyException>(() => this.sut.Save(this.partitionKey, new[] { sameVersion }));

            var stored = (await this.sut.Load(this.partitionKey, 0)).ToList();

            Assert.Equal(1, stored.Count);
            Assert.Equal(1, stored[0].Version);
            Assert.Equal("Payload1", stored[0].Payload);
        }

        [Fact]
        public async Task when_storing_same_version_within_batch_then_aborts_entire_commit()
        {
            this.sut.Save(this.partitionKey, new[] { this.events[0] });

            var sameVersion = new EventData { Version = this.events[0].Version, TypeName = "Test2", Payload = "Payload2" };
            await Assert.ThrowsAsync<ConcurrencyException>(() => this.sut.Save(this.partitionKey, new[] { sameVersion, this.events[1] }));

            var stored = (await this.sut.Load(this.partitionKey, 0)).ToList();

            Assert.Equal(1, stored.Count);
            Assert.Equal(1, stored[0].Version);
            Assert.Equal("Payload1", stored[0].Payload);
        }
    }

    public class given_store_with_events : given_empty_store
    {
        public given_store_with_events()
        {
            this.sut.Save(this.partitionKey, new[] { this.events[0], this.events[1] });
        }
    }

    public class when_getting_pending_events : given_store_with_events
    {
        [Fact]
        public async Task can_get_all_events_for_partition_as_pending()
        {
            var pending = (await this.GetPendingAsyncAndWait(this.sut, this.partitionKey)).ToList();

            Assert.Equal(2, pending.Count);
            Assert.True(pending.All(x => x.SourceType == "Source"));
            Assert.Equal("Unpublished_" + this.events[0].Version.ToString("D10"), pending[0].RowKey);
            Assert.Equal("Payload1", pending[0].Payload);
            Assert.Equal("Test1", pending[0].TypeName);
            Assert.Equal("Unpublished_" + this.events[1].Version.ToString("D10"), pending[1].RowKey);
            Assert.Equal("Payload2", pending[1].Payload);
            Assert.Equal("Test2", pending[1].TypeName);
            Assert.InRange(DateTime.Parse(pending[0].CreationDate, null, DateTimeStyles.RoundtripKind), DateTime.UtcNow.AddSeconds(-10), DateTime.UtcNow);
        }

        [Fact]
        public async Task when_deleting_pending_then_can_get_list_without_item()
        {
            var pending = (await this.GetPendingAsyncAndWait(this.sut, this.partitionKey)).ToList();

            await this.DeletePendingAsyncAndWait(this.sut, pending[0].PartitionKey, pending[0].RowKey);

            pending = (await this.GetPendingAsyncAndWait(this.sut, this.partitionKey)).ToList();

            Assert.Equal(1, pending.Count);
            Assert.Equal("Unpublished_" + this.events[1].Version.ToString("D10"), pending[0].RowKey);
            Assert.Equal("Payload2", pending[0].Payload);
            Assert.Equal("Test2", pending[0].TypeName);
            Assert.Equal("Source", pending[0].SourceType);
        }

        [Fact]
        public async Task can_get_partition_with_pending_events()
        {
            var pending = (await this.sut.GetPartitionsWithPendingEvents()).ToList();

            Assert.Equal(1, pending.Count);
            Assert.Equal(this.partitionKey, pending.Single());
        }

        [Fact]
        public async Task can_delete_item_several_times_for_idempotency()
        {
            var pending = (await this.GetPendingAsyncAndWait(this.sut, this.partitionKey)).ToList();
            await this.DeletePendingAsyncAndWait(this.sut, pending[0].PartitionKey, pending[0].RowKey);
            await this.DeletePendingAsyncAndWait(this.sut, pending[0].PartitionKey, pending[0].RowKey);
            await this.DeletePendingAsyncAndWait(this.sut, pending[0].PartitionKey, pending[0].RowKey);

            pending = (await this.GetPendingAsyncAndWait(this.sut, this.partitionKey)).ToList();

            Assert.Equal(1, pending.Count);
            Assert.Equal("Unpublished_" + this.events[1].Version.ToString("D10"), pending[0].RowKey);
            Assert.Equal("Payload2", pending[0].Payload);
            Assert.Equal("Test2", pending[0].TypeName);
            Assert.Equal("Source", pending[0].SourceType);
        }

        private async Task DeletePendingAsyncAndWait(EventStore sut, string partitionKey, string rowKey)
        {
            var resetEvent = new AutoResetEvent(false);
            await sut.DeletePendingAsync(partitionKey, rowKey, (deleted) => { resetEvent.Set(); Assert.True(deleted); }, Assert.Null);
            Assert.True(resetEvent.WaitOne(5000));
        }

        private async Task<IEnumerable<IEventRecord>> GetPendingAsyncAndWait(EventStore sut, string partitionKey)
        {
            var resetEvent = new AutoResetEvent(false);
            IEnumerable<IEventRecord> results = null;
            bool hasMore = false;
            await sut.GetPendingAsync(partitionKey, (rs, more) =>
            {
                results = rs;
                hasMore = more != null; resetEvent.Set();
            }, Assert.Null);
            Assert.True(resetEvent.WaitOne(6000));
            Assert.False(hasMore);
            return results;
        }
    }

    public class when_getting_pending_events_for_multiple_partitions : given_empty_store
    {
        protected string[] expectedPending;

        // use larger than 1000 in order to force getting continuation tokens, but it takes a lot of time
        private const int NumberOfPartitions = 5; //5000; 

        public when_getting_pending_events_for_multiple_partitions()
        {
            this.expectedPending = new string[NumberOfPartitions];
            for (int i = 0; i < this.expectedPending.Length; i++)
            {
                this.expectedPending[i] = "Test_" + Guid.NewGuid();
                this.sut.Save(this.expectedPending[i], new[] { this.events[0] });
            }
        }

        [Fact]
        public async Task can_get_all_partitions_with_pending_events()
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            var actual = (await this.sut.GetPartitionsWithPendingEvents()).ToList();
            stopWatch.Stop();
            //Debug.WriteLine(stopWatch.ElapsedMilliseconds);
            Assert.Equal(this.expectedPending.Length, actual.Distinct().Count());
            Assert.Equal(this.expectedPending.Length, actual.Intersect(this.expectedPending).Count());
        }
    }
}

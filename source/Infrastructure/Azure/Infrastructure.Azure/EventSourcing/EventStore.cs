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

namespace Infrastructure.Azure.EventSourcing
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Services.Client;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using AutoMapper;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Table.Queryable;

    /// <summary>
    /// Implements an event store using Windows Azure Table Storage.
    /// </summary>
    /// <remarks>
    /// <para> This class works closely related to <see cref="EventStoreBusPublisher"/> and <see cref="AzureEventSourcedRepository{T}"/>, and provides a resilient mechanism to 
    /// store events, and also manage which events are pending for publishing to an event bus.</para>
    /// <para>Ideally, it would be very valuable to provide asynchronous APIs to avoid blocking I/O calls.</para>
    /// <para>See <see href="http://go.microsoft.com/fwlink/p/?LinkID=258557"> Journey chapter 7</see> for more potential performance and scalability optimizations.</para>
    /// </remarks>
    public class EventStore : IEventStore, IPendingEventsQueue
    {
        private const string UnpublishedRowKeyPrefix = "Unpublished_";
        private const string UnpublishedRowKeyPrefixUpperLimit = "Unpublished`";
        private const string RowKeyVersionUpperLimit = "9999999999";
        private readonly CloudStorageAccount account;
        private readonly string tableName;
        private readonly CloudTableClient tableClient;
        private readonly RetryPolicy pendingEventsQueueRetryPolicy;
        private readonly RetryPolicy eventStoreRetryPolicy;

        static EventStore()
        {
            Mapper.CreateMap<EventTableEntity, EventData>();
            Mapper.CreateMap<EventData, EventTableEntity>();
        }

        public EventStore(CloudStorageAccount account, string tableName)
        {
            if (account == null) throw new ArgumentNullException(nameof(account));
            if (tableName == null) throw new ArgumentNullException(nameof(tableName));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException(nameof(tableName));

            this.account = account;
            this.tableName = tableName;
            this.tableClient = account.CreateCloudTableClient();
            this.tableClient.DefaultRequestOptions.RetryPolicy = new NoRetry();

            // TODO: This could be injected.
            var backgroundRetryStrategy = new ExponentialBackoff(10, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(1));
            var blockingRetryStrategy = new Incremental(3, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
            this.pendingEventsQueueRetryPolicy = new RetryPolicy<StorageTransientErrorDetectionStrategy>(backgroundRetryStrategy);
            this.pendingEventsQueueRetryPolicy.Retrying += (s, e) =>
            {
                var handler = this.Retrying;
                if (handler != null)
                {
                    handler(this, EventArgs.Empty);
                }

                Trace.TraceWarning("An error occurred in attempt number {1} to access table storage (PendingEventsQueue): {0}", e.LastException.Message, e.CurrentRetryCount);
            };
            this.eventStoreRetryPolicy = new RetryPolicy<StorageTransientErrorDetectionStrategy>(blockingRetryStrategy);
            this.eventStoreRetryPolicy.Retrying += (s, e) => Trace.TraceWarning(
                "An error occurred in attempt number {1} to access table storage (EventStore): {0}",
                e.LastException.Message,
                e.CurrentRetryCount);

            var tableReference = this.tableClient.GetTableReference(tableName);

            this.eventStoreRetryPolicy.ExecuteAction(() => tableReference.CreateIfNotExists());
        }

        /// <summary>
        /// Notifies that the sender is retrying due to a transient fault.
        /// </summary>
        public event EventHandler Retrying;

        public async Task<IEnumerable<EventData>> LoadAsync(string partitionKey, int version)
        {
            var minRowKey = version.ToString("D10");
            var query = this.GetEntitiesQuery(partitionKey, minRowKey, RowKeyVersionUpperLimit);
            var results = new List<EventData>();

            TableContinuationToken continuationToken = null;

            do
            {
                var partial = await this.eventStoreRetryPolicy.ExecuteAsync(() => query.ExecuteSegmentedAsync(continuationToken));

                results.AddRange(partial.Select(x => Mapper.Map(x, new EventData { Version = int.Parse(x.RowKey) })));

                continuationToken = partial.ContinuationToken;
            } while (continuationToken != null);

            return results;
        }

        public async Task SaveAsync(string partitionKey, IEnumerable<EventData> events)
        {
            var table = this.tableClient.GetTableReference(this.tableName);

            var batchOperation = new TableBatchOperation();

            foreach (var eventData in events)
            {
                string creationDate = DateTime.UtcNow.ToString("o");
                var formattedVersion = eventData.Version.ToString("D10");

                batchOperation.Insert(Mapper.Map(eventData, new EventTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = formattedVersion,
                    CreationDate = creationDate,
                }));

                // Add a duplicate of this event to the Unpublished "queue"
                batchOperation.Insert(Mapper.Map(eventData, new EventTableEntity
                {
                    PartitionKey = partitionKey,
                    RowKey = UnpublishedRowKeyPrefix + formattedVersion,
                    CreationDate = creationDate,
                }));
            }

            try
            {
                await this.eventStoreRetryPolicy.ExecuteAction(() => table.ExecuteBatchAsync(batchOperation));
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    throw new ConcurrencyException();
                }

                throw;
            }
        }

        /// <summary>
        /// Gets the pending events for publishing asynchronously using delegate continuations.
        /// </summary>
        /// <param name="partitionKey">The partition key to get events from.</param>
        public async Task<IEnumerable<IEventRecord>> GetPendingAsync(string partitionKey)
        {
            var query = this.GetEntitiesQuery(partitionKey, UnpublishedRowKeyPrefix, UnpublishedRowKeyPrefixUpperLimit);

            var continuationToken = (TableContinuationToken)null;
            var eventRecords = new List<IEventRecord>();

            do
            {
                var result = await this.pendingEventsQueueRetryPolicy.ExecuteAsync(() => query.ExecuteSegmentedAsync(continuationToken));
                eventRecords.AddRange(result);
                continuationToken = result.ContinuationToken;
            } while (continuationToken != null);

            return eventRecords;
        }

        /// <summary>
        /// Deletes the specified pending event from the queue.
        /// </summary>
        /// <param name="partitionKey">The partition key of the event.</param>
        /// <param name="rowKey">The partition key of the event.</param>
        public async Task<bool> DeletePendingAsync(string partitionKey, string rowKey)
        {
            var table = this.tableClient.GetTableReference(this.tableName);
            var item = new EventTableEntity { PartitionKey = partitionKey, RowKey = rowKey, ETag = "*"};

            var operation = TableOperation.Delete(item);

            try
            {
                var r = await this.pendingEventsQueueRetryPolicy.ExecuteAsync(() => table.ExecuteAsync(operation));

                return r.HttpStatusCode == (int)HttpStatusCode.NoContent;
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                    return true;

                throw;
            }
        }

        /// <summary>
        /// Gets the list of all partitions that have pending unpublished events.
        /// </summary>
        /// <returns>The list of all partitions.</returns>
        public async Task<IEnumerable<string>> GetPartitionsWithPendingEvents()
        {
            var table = this.tableClient.GetTableReference(this.tableName);

            var query = table.CreateQuery<EventTableEntity>()
                             .Where(
                                 x =>
                                     x.RowKey.CompareTo(UnpublishedRowKeyPrefix) >= 0 &&
                                     x.RowKey.CompareTo(UnpublishedRowKeyPrefixUpperLimit) <= 0)
                             .Select(x => x.PartitionKey).AsTableQuery();

            var result = new BlockingCollection<string>();
            var tokenSource = new CancellationTokenSource();
            TableContinuationToken continuationToken = null;

            do
            {
                try
                {
                    var segment = await this.pendingEventsQueueRetryPolicy.ExecuteAsync(() => query.ExecuteSegmentedAsync(null));
                    foreach (var key in segment.Results.Distinct())
                    {
                        result.Add(key);
                    }

                    continuationToken = segment.ContinuationToken;
                }
                catch
                {
                    tokenSource.Cancel();
                    throw;
                }
            } while (continuationToken != null);

            result.CompleteAdding();

            return result.GetConsumingEnumerable(tokenSource.Token);
        }

        private TableQuery<EventTableEntity> GetEntitiesQuery(string partitionKey, string minRowKey, string maxRowKey)
        {
            var table = this.tableClient.GetTableReference(this.tableName);

            var query = table
                .CreateQuery<EventTableEntity>()
                .Where(
                    x =>
                    x.PartitionKey == partitionKey && x.RowKey.CompareTo(minRowKey) >= 0 && x.RowKey.CompareTo(maxRowKey) <= 0);

            return query.AsTableQuery();
        }
    }
}

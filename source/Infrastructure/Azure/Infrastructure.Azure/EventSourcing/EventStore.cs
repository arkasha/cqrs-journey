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
        private readonly Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.RetryPolicy pendingEventsQueueRetryPolicy;
        private readonly Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling.RetryPolicy eventStoreRetryPolicy;

        static EventStore()
        {
            Mapper.CreateMap<EventTableEntity, EventData>();
            Mapper.CreateMap<EventData, EventTableEntity>();
        }

        public EventStore(CloudStorageAccount account, string tableName)
        {
            if (account == null) throw new ArgumentNullException("account");
            if (tableName == null) throw new ArgumentNullException("tableName");
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName");

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

        public async Task<IEnumerable<EventData>> Load(string partitionKey, int version)
        {
            var minRowKey = version.ToString("D10");
            var query = this.GetEntitiesQuery(partitionKey, minRowKey, RowKeyVersionUpperLimit);
            // TODO: use async APIs, continuation tokens
            var all = this.eventStoreRetryPolicy.ExecuteAction(() => query.Execute());
            return all.Select(x => Mapper.Map(x, new EventData { Version = int.Parse(x.RowKey) }));
        }

        public async Task Save(string partitionKey, IEnumerable<EventData> events)
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
        /// <param name="successCallback">The callback that will be called if the data is successfully retrieved. 
        /// The first argument of the callback is the list of pending events.
        /// The second argument is true if there are more records that were not retrieved.</param>
        /// <param name="exceptionCallback">The callback used if there is an exception that does not allow to continue.</param>
        public async Task GetPendingAsync(string partitionKey, Action<IEnumerable<IEventRecord>, TableContinuationToken> successCallback, Action<Exception> exceptionCallback)
        {
            var query = this.GetEntitiesQuery(partitionKey, UnpublishedRowKeyPrefix, UnpublishedRowKeyPrefixUpperLimit);
            await this.pendingEventsQueueRetryPolicy.ExecuteAsync(() => query.ExecuteSegmentedAsync(null)).ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    exceptionCallback(t.Exception.InnerException);
                }
                else if (t.IsCompleted)
                {
                    var rs = t.Result;
                    var all = rs.Results.ToList();
                    successCallback(rs.Results, rs.ContinuationToken);
                }
            });
        }

        /// <summary>
        /// Deletes the specified pending event from the queue.
        /// </summary>
        /// <param name="partitionKey">The partition key of the event.</param>
        /// <param name="rowKey">The partition key of the event.</param>
        /// <param name="successCallback">The callback that will be called if the data is successfully retrieved.
        /// The argument specifies if the row was deleted. If false, it means that the row did not exist.
        /// </param>
        /// <param name="exceptionCallback">The callback used if there is an exception that does not allow to continue.</param>
        public async Task DeletePendingAsync(string partitionKey, string rowKey, Action<bool> successCallback, Action<Exception> exceptionCallback)
        {
            var table = this.tableClient.GetTableReference(this.tableName);
            var item = new EventTableEntity { PartitionKey = partitionKey, RowKey = rowKey, ETag = "*"};

            var operation = TableOperation.Delete(item);

            await this.pendingEventsQueueRetryPolicy.ExecuteAsync(() => table.ExecuteAsync(operation)).ContinueWith(t =>
            {
                if (t.IsFaulted)
                {
                    var inner = t.Exception?.InnerException as StorageException;

                    if (inner != null && inner.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                        successCallback(true);
                    else
                        exceptionCallback(t.Exception?.InnerException);
                }
                else if (!t.IsCanceled)
                {
                    successCallback(t.Result.HttpStatusCode == (int)HttpStatusCode.NoContent);
                }
            });
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

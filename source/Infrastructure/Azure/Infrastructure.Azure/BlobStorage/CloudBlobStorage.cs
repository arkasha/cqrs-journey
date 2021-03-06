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

namespace Infrastructure.Azure.BlobStorage
{
    using System;
    using System.Diagnostics;
    using System.Net;
    using System.Threading.Tasks;
    using Infrastructure.BlobStorage;
    using Microsoft.Practices.EnterpriseLibrary.TransientFaultHandling;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.RetryPolicies;

    public class CloudBlobStorage : IBlobStorage
    {
        private readonly CloudStorageAccount account;
        private readonly string rootContainerName;
        private readonly CloudBlobClient blobClient;
        private readonly RetryPolicy<StorageTransientErrorDetectionStrategy> readRetryPolicy;
        private readonly RetryPolicy<StorageTransientErrorDetectionStrategy> writeRetryPolicy;

        public CloudBlobStorage(CloudStorageAccount account, string rootContainerName)
        {
            this.account = account;
            this.rootContainerName = rootContainerName;

            this.blobClient = account.CreateCloudBlobClient();
            this.blobClient.DefaultRequestOptions.RetryPolicy = new NoRetry();

            this.readRetryPolicy = new RetryPolicy<StorageTransientErrorDetectionStrategy>(new Incremental(1, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1)));
            this.readRetryPolicy.Retrying += (s, e) => Trace.TraceWarning("An error occurred in attempt number {1} to read from blob storage: {0}", e.LastException.Message, e.CurrentRetryCount);
            this.writeRetryPolicy = new RetryPolicy<StorageTransientErrorDetectionStrategy>(new FixedInterval(1, TimeSpan.FromSeconds(10)) { FastFirstRetry = false });
            this.writeRetryPolicy.Retrying += (s, e) => Trace.TraceWarning("An error occurred in attempt number {1} to write to blob storage: {0}", e.LastException.Message, e.CurrentRetryCount);

            var containerReference = this.blobClient.GetContainerReference(this.rootContainerName);
            this.writeRetryPolicy.ExecuteAction(() => containerReference.CreateIfNotExists());
        }

        public async Task<byte[]> FindAsync(string id)
        {
            var containerReference = this.blobClient.GetContainerReference(this.rootContainerName);
            var blobReference = containerReference.GetBlobReference(id);
            
            return await this.readRetryPolicy.ExecuteAsync(async () =>
                {
                    try
                    {
                        await blobReference.FetchAttributesAsync();
                        var buffer = new byte[blobReference.Properties.Length];
                        await blobReference.DownloadToByteArrayAsync(buffer, 0);
                        return buffer;
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                        {
                            return null;
                        }

                        throw;
                    }
                });
        }

        public async Task SaveAsync(string id, string contentType, byte[] blob)
        {
            var client = this.account.CreateCloudBlobClient();
            var containerReference = client.GetContainerReference(this.rootContainerName);

            CloudBlockBlob blobReference = containerReference.GetBlockBlobReference(id);

            await this.writeRetryPolicy.ExecuteAsync(async () =>
                {
                    await blobReference.UploadFromByteArrayAsync(blob, 0, blob.Length);

                });
        }

        public async Task DeleteAsync(string id)
        {
            var client = this.account.CreateCloudBlobClient();
            var containerReference = client.GetContainerReference(this.rootContainerName);
            var blobReference = containerReference.GetBlobReference(id);

            await this.writeRetryPolicy.ExecuteAsync(async () =>
                {
                    try
                    {
                       await blobReference.DeleteIfExistsAsync();
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode != (int)HttpStatusCode.NotFound)
                        {
                            throw;
                        }
                    }
                });
        }
    }
}

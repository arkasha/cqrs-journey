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

namespace Infrastructure.Azure
{
    using Infrastructure.Azure.Messaging;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;

    /// <summary>
    /// Admin helpers for tests.
    /// </summary>
    public static class BusSettingsExtensions
    {
        public static MessageReceiver CreateMessageReceiver(this ServiceBusSettings settings, string topic, string subscription)
        {
            var messagingFactory = MessagingFactory.CreateFromConnectionString(settings.ConnectionString);

            return messagingFactory.CreateMessageReceiver(SubscriptionClient.FormatDeadLetterPath(topic, subscription));
        }

        public static SubscriptionClient CreateSubscriptionClient(this ServiceBusSettings settings, string topic, string subscription, ReceiveMode mode = ReceiveMode.PeekLock)
        {
            var messagingFactory = MessagingFactory.CreateFromConnectionString(settings.ConnectionString);

            return messagingFactory.CreateSubscriptionClient(topic, subscription, mode);
        }

        public static TopicClient CreateTopicClient(this ServiceBusSettings settings, string topic)
        {
            var messagingFactory = MessagingFactory.CreateFromConnectionString(settings.ConnectionString);

            return messagingFactory.CreateTopicClient(topic);
        }


        public static void CreateTopic(this ServiceBusSettings settings, string topic)
        {
            NamespaceManager.CreateFromConnectionString(settings.ConnectionString).CreateTopic(topic);
        }

        public static void CreateSubscription(this ServiceBusSettings settings, string topic, string subscription)
        {
            CreateTopic(settings, topic);

            NamespaceManager.CreateFromConnectionString(settings.ConnectionString).CreateSubscription(topic, subscription);
        }

        public static void CreateSubscription(this ServiceBusSettings settings, SubscriptionDescription description)
        {
            CreateTopic(settings, description.TopicPath);

            NamespaceManager.CreateFromConnectionString(settings.ConnectionString).CreateSubscription(description);
        }

        public static void TryDeleteSubscription(this ServiceBusSettings settings, string topic, string subscription)
        {
            try
            {
                NamespaceManager.CreateFromConnectionString(settings.ConnectionString).DeleteSubscription(topic, subscription);
            }
            catch { }
        }

        public static void TryDeleteTopic(this ServiceBusSettings settings, string topic)
        {
            try
            {
                NamespaceManager.CreateFromConnectionString(settings.ConnectionString).DeleteTopic(topic);
            }
            catch { }
        }
    }
}

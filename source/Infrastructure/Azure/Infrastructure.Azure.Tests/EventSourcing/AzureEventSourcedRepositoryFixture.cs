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

namespace Infrastructure.Azure.Tests.EventSourcing.AzureEventSourcedRepositoryFixture
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.Caching;
    using System.Threading.Tasks;
    using Infrastructure.Azure.EventSourcing;
    using Infrastructure.Azure.Tests.Mocks;
    using Infrastructure.EventSourcing;
    using Infrastructure.Serialization;
    using Moq;
    using Xunit;

    public class when_saving_entity
    {
        private Guid id;
        private Mock<IEventStore> eventStore;
        private Mock<IEventStoreBusPublisher> publisher;

        public when_saving_entity()
        {
            this.eventStore = new Mock<IEventStore>();
            this.publisher = new Mock<IEventStoreBusPublisher>();
            var sut = new AzureEventSourcedRepository<TestEntity>(this.eventStore.Object, this.publisher.Object, new JsonTextSerializer(), new StandardMetadataProvider(), null);
            this.id = Guid.NewGuid();
            var entity = new TestEntity
            {
                Id = this.id,
                Events =
                    {
                        new TestEvent { SourceId = this.id, Version = 1, Foo = "Bar" },
                        new TestEvent { SourceId = this.id, Version = 2, Foo = "Baz" }
                    }
            };

            sut.SaveAsync(entity, "correlation");
        }

        [Fact]
        public void then_stores_in_event_store()
        {
            this.eventStore.Verify(
                s => s.SaveAsync(
                    It.IsAny<string>(),
                    It.Is<IEnumerable<EventData>>(
                        x =>
                            x.Count() == 2
                            && x.First().Version == 1
                            && x.First().SourceId == this.id.ToString()
                            && x.First().SourceType == "TestEntity"
                            && x.First().TypeName == "TestEvent"
                            && x.First().Payload.Contains("Bar")
                            && x.First().CorrelationId == "correlation"
                            && x.Last().Version == 2
                            && x.Last().SourceId == this.id.ToString()
                            && x.Last().SourceType == "TestEntity"
                            && x.Last().Payload.Contains("Baz")
                            && x.Last().CorrelationId == "correlation")));
        }

        [Fact]
        public void then_uses_composed_partition_key()
        {
            this.eventStore.Verify(s => s.SaveAsync("TestEntity_" + this.id.ToString(), It.IsAny<IEnumerable<EventData>>()));
        }

        [Fact]
        public void then_notifies_publisher_about_the_pending_partition_key()
        {
            this.publisher.Verify(s => s.Send("TestEntity_" + this.id.ToString(), 2));
        }
    }

    public class when_saving_memento_originator_entity
    {
        private Guid id;
        private Mock<IEventStore> eventStore;
        private Mock<IEventStoreBusPublisher> publisher;
        private IMemento memento;
        private MemoryCache cache;

        public when_saving_memento_originator_entity()
        {
            this.eventStore = new Mock<IEventStore>();
            this.publisher = new Mock<IEventStoreBusPublisher>();
            this.cache = new MemoryCache(Guid.NewGuid().ToString());
            var sut = new AzureEventSourcedRepository<TestOriginatorEntity>(this.eventStore.Object, this.publisher.Object, new JsonTextSerializer(), new StandardMetadataProvider(), this.cache);
            this.id = Guid.NewGuid();
            this.memento = Mock.Of<IMemento>();
            var entity = new TestOriginatorEntity
            {
                Id = this.id,
                Events =
                    {
                        new TestEvent { SourceId = this.id, Version = 1, Foo = "Bar" },
                        new TestEvent { SourceId = this.id, Version = 2, Foo = "Baz" }
                    },
                Memento = this.memento,
            };

            sut.SaveAsync(entity, "correlation");
        }

        [Fact]
        public void then_stores_in_event_store()
        {
            this.eventStore.Verify(
                s => s.SaveAsync(
                    It.IsAny<string>(),
                    It.Is<IEnumerable<EventData>>(
                        x =>
                            x.Count() == 2
                            && x.First().Version == 1
                            && x.First().SourceId == this.id.ToString()
                            && x.First().SourceType == "TestOriginatorEntity"
                            && x.First().TypeName == "TestEvent"
                            && x.First().Payload.Contains("Bar")
                            && x.First().CorrelationId == "correlation"
                            && x.Last().Version == 2
                            && x.Last().SourceId == this.id.ToString()
                            && x.Last().SourceType == "TestOriginatorEntity"
                            && x.Last().Payload.Contains("Baz")
                            && x.Last().CorrelationId == "correlation")));
        }

        [Fact]
        public void then_uses_composed_partition_key()
        {
            this.eventStore.Verify(s => s.SaveAsync("TestOriginatorEntity_" + this.id.ToString(), It.IsAny<IEnumerable<EventData>>()));
        }

        [Fact]
        public void then_notifies_publisher_about_the_pending_partition_key()
        {
            this.publisher.Verify(s => s.Send("TestOriginatorEntity_" + this.id.ToString(), 2));
        }

        [Fact]
        public void then_stores_memento_in_cache()
        {
            var cached = (Tuple<IMemento, DateTime?>)this.cache["TestOriginatorEntity_" + this.id.ToString()];
            Assert.Equal(this.memento, cached.Item1);
        }
    }

    public class when_reading_entity
    {
        private Guid id;

        [Fact]
        public async Task when_reading_entity_then_rehydrates()
        {
            var events = new IVersionedEvent[]
                             {
                                 new TestEvent { SourceId = this.id, Version = 1, Foo = "Bar" },
                                 new TestEvent { SourceId = this.id, Version = 2, Foo = "Baz" }                              
                             };
            var serialized = events.Select(x => new EventData { Version = x.Version, Payload = Serialize(x) });
            this.id = Guid.NewGuid();
            var eventStore = new Mock<IEventStore>();
            eventStore.Setup(x => x.LoadAsync(It.IsAny<string>(), It.IsAny<int>())).Returns(Task.FromResult(serialized));
            var sut = new AzureEventSourcedRepository<TestEntity>(eventStore.Object, Mock.Of<IEventStoreBusPublisher>(), new JsonTextSerializer(), new StandardMetadataProvider(), null);

            var entity = await sut.FindAsync(this.id);

            Assert.NotNull(entity);
            Assert.Equal(this.id, entity.Id);
            Assert.Equal(events, entity.History, new TestEventComparer());
        }

        private static string Serialize(object graph)
        {
            var serializer = new JsonTextSerializer();
            var writer = new StringWriter();
            serializer.Serialize(writer, graph);
            return writer.ToString();
        }
    }

    public class when_reading_cached_memento_originator_entity
    {
        private Guid id;
        private IMemento memento;

        [Fact]
        public async Task when_reading_entity_then_rehydrates()
        {
            var newEvents = new IVersionedEvent[]
                             {
                                 new TestEvent { SourceId = this.id, Version = 2, Foo = "Baz" }                              
                             };
            var serialized = newEvents.Select(x => new EventData { Version = x.Version, Payload = Serialize(x) });
            this.id = Guid.NewGuid();
            var eventStore = new Mock<IEventStore>();
            this.memento = Mock.Of<IMemento>(x => x.Version == 1);
            var cache = new MemoryCache(Guid.NewGuid().ToString());
            cache.Add("TestOriginatorEntity_" + this.id.ToString(), new Tuple<IMemento, DateTime?>(this.memento, null), DateTimeOffset.UtcNow.AddMinutes(10));

            eventStore.Setup(x => x.LoadAsync(It.IsAny<string>(), 2)).Returns(Task.FromResult(serialized));
            var sut = new AzureEventSourcedRepository<TestOriginatorEntity>(eventStore.Object, Mock.Of<IEventStoreBusPublisher>(), new JsonTextSerializer(), new StandardMetadataProvider(), cache);

            var entity = await sut.FindAsync(this.id);

            Assert.NotNull(entity);
            Assert.Equal(this.id, entity.Id);
            Assert.Equal(this.memento, entity.Memento);
            Assert.Equal(newEvents, entity.History, new TestEventComparer());
        }

        private static string Serialize(object graph)
        {
            var serializer = new JsonTextSerializer();
            var writer = new StringWriter();
            serializer.Serialize(writer, graph);
            return writer.ToString();
        }
    }

    public class when_reading_inexistant_entity
    {
        private Guid id;
        private Mock<IEventStore> eventStore;
        private AzureEventSourcedRepository<TestEntity> sut;

        public when_reading_inexistant_entity()
        {
            this.eventStore = new Mock<IEventStore>();
            this.sut = new AzureEventSourcedRepository<TestEntity>(this.eventStore.Object, Mock.Of<IEventStoreBusPublisher>(), new JsonTextSerializer(), new StandardMetadataProvider(), null);
            this.id = Guid.NewGuid();
        }

        [Fact]
        public async Task when_finding_then_returns_null()
        {
            Assert.Null(await this.sut.FindAsync(this.id));
        }

        [Fact]
        public async Task when_getting_then_throws()
        {
            var actual = await Assert.ThrowsAsync<EntityNotFoundException>(() => this.sut.Get(this.id));
            Assert.Equal(this.id, actual.EntityId);
            Assert.Equal("TestEntity", actual.EntityType);
        }
    }
}

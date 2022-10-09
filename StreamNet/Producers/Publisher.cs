using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;
using StreamNet.Serializers;

namespace StreamNet.Producers
{
    public class Publisher : IPublisher
    {
        private readonly ILogger<Publisher> _logger;

        public Publisher(ILogger<Publisher> logger)
        {
            _logger = logger;
            Settings.GetInstance();
        }

        public async Task ProduceAsync<T>(T message, string? topicName = null)
        {
            try
            {
                _ = Task.Run(async () =>
                {
                    using var producerBuilder = new ProducerBuilder<Null, T>(Settings.ProducerConfig)
                        .SetValueSerializer(new Serializer<T>()).Build();
                    topicName ??= message?.GetType().FullName;
                    await producerBuilder.ProduceAsync(topicName, new Message<Null, T> { Value = message });
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "A unexpected error occurred in {method} with params {@input}", nameof(ProduceAsync), message);
                throw;
            }
        }

        public async Task ProduceAvroAsync<T>(T message, string? topicName = null)
        {
            try
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        using (var schemaRegistry = new CachedSchemaRegistryClient(Settings.SchemaRegistryConfig))
                        {
                            using (var producer = new ProducerBuilder<Null, T>(Settings.ProducerConfig)
                                       .SetValueSerializer(new AvroSerializer<T>(schemaRegistry))
                                       .Build())
                            {
                                topicName ??= message?.GetType().FullName;
                                await producer.ProduceAsync(topicName, new Message<Null, T> { Value = message });
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "A unexpected error occurred in {method} with params {@input}", nameof(ProduceAvroAsync), message);
                        throw;
                    }
                });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        internal async Task ProduceAsyncDeadLetter<T>(T message, string? topicName = null)
        {
            try
            {
                Settings.GetInstance();
                using var producerBuilder = new ProducerBuilder<Null, T>(Settings.ProducerConfig)
                    .SetValueSerializer(new Serializer<T>()).Build();
                topicName ??= message?.GetType().FullName;

                if (string.IsNullOrEmpty(topicName))
                    return;

                await producerBuilder.ProduceAsync($"{topicName}_dead_letter",
                    new Message<Null, T> { Value = message });
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}
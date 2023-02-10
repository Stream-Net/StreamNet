using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using StreamNet.Extensions.Encoders;
using StreamNet.Serializers;

namespace StreamNet.Producers
{
    public class Publisher : IPublisher
    {
        public Publisher() => Settings.GetInstance();

        public async Task ProduceAvroAsync<T>(T message, string? topicName = null)
        {
            try
            {
                Task.Run(async () =>
                {
                    try
                    {
                        topicName ??= message?.GetType().FullName;
                        using var producer = new ProducerBuilder<Null, byte[]>(Settings.ProducerConfig).Build();
                        var messageByteArray = ByteArrayEncoder.EncodeToByteArray(message);
                        var serializer = new AvroSerializer<byte[]>(new CachedSchemaRegistryClient(Settings.SchemaRegistryConfig));
                        var serializedMessage = await serializer.SerializeAsync(messageByteArray, new SerializationContext()); 
                        await producer
                            .ProduceAsync(topicName, new Message<Null, byte[]>
                            {
                                Value = serializedMessage
                            });
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
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

        public async Task ProduceAsync<T>(T message, string? topicName = null)
        {
            try
            {
                await Task.Run(async () =>
                {
                    using var producerBuilder = new ProducerBuilder<Null, T>(Settings.ProducerConfig)
                        .SetValueSerializer(new Serializer<T>()).Build();
                    topicName ??= message?.GetType().FullName;
                    await producerBuilder.ProduceAsync(topicName, new Message<Null, T> { Value = message });
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
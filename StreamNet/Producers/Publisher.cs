using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using StreamNet.Serializers;

namespace StreamNet.Producers
{
    public class Publisher : IPublisher
    {
        public Publisher() => Settings.GetInstance();

        public async Task ProduceAsync<T>(T message, string? topicName = null)
        {
            try
            {
                Task.Run(async () =>
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
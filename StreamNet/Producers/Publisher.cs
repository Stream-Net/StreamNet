using Confluent.Kafka;
using StreamNet.Serializers;

namespace StreamNet.Producers;

public class Publisher : IPublisher
{
    public async Task ProduceAsync<T>(T message)
    {
        try
        {
            Settings.GetInstance();
            using var producerBuilder = new ProducerBuilder<Null, T>(Settings.ProducerConfig)
                .SetValueSerializer(new Serializer<T>()).Build();
            await producerBuilder.ProduceAsync(message?.GetType().FullName, new Message<Null, T> {Value = message});
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    internal async Task ProduceAsyncDeadLetter<T>(T message)
    {
        try
        {
            Settings.GetInstance();
            using var producerBuilder = new ProducerBuilder<Null, T>(Settings.ProducerConfig)
                .SetValueSerializer(new Serializer<T>()).Build();
            await producerBuilder.ProduceAsync($"{message?.GetType().FullName}_dead_letter",
                new Message<Null, T> {Value = message});
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }
}
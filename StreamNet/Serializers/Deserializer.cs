using Confluent.Kafka;

namespace StreamNet.Serializers;

public class Deserializer<TEvent> : IDeserializer<TEvent>
{
    public TEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        var deserializedWeatherForecast = System.Text.Json.JsonSerializer.Deserialize<TEvent>(data)!;
        return deserializedWeatherForecast;
    }
}
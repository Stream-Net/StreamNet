using System.Text;
using Confluent.Kafka;

namespace StreamNet.Serializers;

public class Serializer<TEvent> : ISerializer<TEvent>
{
    public byte[] Serialize(TEvent data, SerializationContext context)
    {
        var deserializedWeatherForecast = System.Text.Json.JsonSerializer.Serialize(data);
        return Encoding.ASCII.GetBytes(deserializedWeatherForecast);
    }
}
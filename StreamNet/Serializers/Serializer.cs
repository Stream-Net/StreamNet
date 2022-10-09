using System.Text;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;

namespace StreamNet.Serializers
{
    public class Serializer<TEvent> : ISerializer<TEvent>
    {
        public byte[] Serialize(TEvent data, SerializationContext context)
        {
            var serializedData = System.Text.Json.JsonSerializer.Serialize(data);
            return Encoding.ASCII.GetBytes(serializedData);
        }
    }
    
    public class AvroSpecficSerializer<TEvent> : ISerializer<TEvent>
    {
        public byte[] Serialize(TEvent data, SerializationContext context)
        {
            var serializedData = System.Text.Json.JsonSerializer.Serialize(data);
            return Encoding.ASCII.GetBytes(serializedData);
        }
    }
}
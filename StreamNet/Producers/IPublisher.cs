using System.Threading.Tasks;

namespace StreamNet.Producers
{
    public interface IPublisher
    {
        Task ProduceAsync<T>(T message, string? topicName = null);
        Task ProduceAvroAsync<T>(T message, string? topicName = null);
    }
}
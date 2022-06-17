using System.Threading.Tasks;

namespace StreamNet.Producers
{
    public interface IPublisher
    {
        Task ProduceAsync<T>(T message, string? topicName = null);
    }
}
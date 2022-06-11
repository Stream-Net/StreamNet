namespace StreamNet.Producers;

public interface IPublisher
{
    Task ProduceAsync<T>(T message);
}
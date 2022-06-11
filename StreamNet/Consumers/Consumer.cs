using System.Diagnostics;
using Confluent.Kafka;
using StreamNet.Serializers;
using Microsoft.Extensions.Hosting;

namespace StreamNet.Consumers;

public abstract class Consumer<TEvent> : IHostedService
{
    private ConsumerConfig _config;
    protected IConsumer<string, TEvent> _consumer;
    protected ConsumeResult<string, TEvent> _consumeResult;
    protected string ConsumerGroupId { get; }
    protected TEvent Message { get; set; }

    protected Consumer(string consumerGroupId) => ConsumerGroupId = consumerGroupId;

    protected abstract Task HandleAsync();
    
    protected async Task Consume()
    {
        try
        {
            _consumer.Subscribe(typeof(TEvent).Name);
            var cancelToken = new CancellationTokenSource();
            try
            {
                while (true)
                {
                    _consumeResult = _consumer.Consume(cancelToken.Token);
                    Message = _consumeResult.Message.Value;
                    Console.WriteLine($"Processing message: {Message}", Message);
                    await HandleAsync();
                }
            }
            catch (OperationCanceledException)
            {
                _consumer.Close();
                throw;
            }
        }
        catch (Exception ex)
        {
            Debug.WriteLine(ex.Message);
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        Settings.GetInstance();
        var consumerConfig = Settings.ConsumerConfig;
        consumerConfig.GroupId = ConsumerGroupId;
        consumerConfig.EnableAutoCommit = true;
        consumerConfig.EnableAutoOffsetStore = true;
        _config = consumerConfig;
        _consumer = new ConsumerBuilder<string, TEvent>(_config)
            .SetValueDeserializer(new Deserializer<TEvent>())
            .Build();
        
        await Task.Factory.StartNew(() =>
        {
            Consume();
        });
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
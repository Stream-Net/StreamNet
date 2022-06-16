using Confluent.Kafka;
using StreamNet.Serializers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using StreamNet.Producers;

namespace StreamNet.Consumers;

public abstract class Consumer<TEvent> : IHostedService
{
    protected readonly ILogger<Consumer<TEvent>> _logger;
    private ConsumerConfig _config;
    protected IConsumer<string, TEvent> _consumer;
    protected ConsumeResult<string, TEvent> _consumeResult;
    protected string ConsumerGroupId { get; }
    protected TEvent Message { get; set; }
    private AsyncRetryPolicy _retryPolicy;

    protected Consumer(ILogger<Consumer<TEvent>> logger, string consumerGroupId)
    {
        Settings.GetInstance();
        _logger = logger;
        ConsumerGroupId = consumerGroupId;
        _retryPolicy = Policy.Handle<Exception>()
            .WaitAndRetryAsync(retryCount: Settings.RetryCount,
                sleepDurationProvider: _ => TimeSpan.FromSeconds(Settings.TimeToRetryInSeconds));
    }

    protected abstract Task HandleAsync();

    protected async Task Consume()
    {
        try
        {
            _consumer.Subscribe(typeof(TEvent).FullName);
            var cancelToken = new CancellationTokenSource();
            try
            {
                while (true)
                {
                    _consumeResult = _consumer.Consume(cancelToken.Token);
                    Message = _consumeResult.Message.Value;
                    _logger.LogInformation("Processing message: {@Message}", Message);
                    await _retryPolicy.ExecuteAsync(async () => { await HandleAsync(); });
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
            _logger.LogInformation("Sending message to dead-letter {@Message}", Message);
            await new Publisher().ProduceAsyncDeadLetter(Message);
            Consume();
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

        Task.Factory.StartNew(async () => { return Consume(); });
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
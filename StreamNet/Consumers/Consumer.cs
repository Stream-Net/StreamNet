using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using StreamNet.Serializers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;
using StreamNet.Producers;
using StreamNet.UnitTestingHelpers;

namespace StreamNet.Consumers
{
    public abstract class Consumer<TEvent> : IHostedService
    {
        private ConsumerConfig _config;
        private AsyncRetryPolicy _retryPolicy;
        protected IConsumer<string, TEvent> _consumer;
        protected ConsumeResult<string, TEvent> _consumeResult;
        private string ConsumerGroupId { get; set; }
        private string TopicName { get; set; }
        protected TEvent Message { get; set; }
        protected readonly ILogger<Consumer<TEvent>> _logger;

        protected Consumer(ILogger<Consumer<TEvent>> logger)
        {
            Settings.GetInstance();
            _logger = logger;
            
            // if (UnitTestDetector.IsRunningFromUnitTesting())
            //     return;
            
            SetConsumerId();
            SetTopicName();
            _retryPolicy = Policy.Handle<Exception>()
                .WaitAndRetryAsync(retryCount: Settings.RetryCount,
                    sleepDurationProvider: _ => TimeSpan.FromSeconds(Settings.TimeToRetryInSeconds));
        }

        private void SetTopicName()
        {
            var topicNameFromAttribute =
                ((TopicNameAttribute) Attribute.GetCustomAttribute(GetType(), typeof(TopicNameAttribute))!)?.TopicName;
            TopicName = string.IsNullOrEmpty(topicNameFromAttribute) ? typeof(TEvent).FullName : topicNameFromAttribute;
        }

        private void SetConsumerId()
        {
            var consumerGroup =
                ((ConsumerGroupAttribute) Attribute.GetCustomAttribute(GetType(), typeof(ConsumerGroupAttribute))!)
                .ConsumerGroup;
            if (string.IsNullOrEmpty(consumerGroup) && !UnitTestDetector.IsRunningFromUnitTesting())
                throw new ArgumentNullException($"ConsumerGroup attribute is required!");
            ConsumerGroupId = consumerGroup;
        }


        public abstract Task HandleAsync();

        protected async Task Consume()
        {
            try
            {
                _consumer.Subscribe(TopicName);
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
                await new Publisher().ProduceAsyncDeadLetter(Message, TopicName);
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
}
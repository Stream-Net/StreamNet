using StreamNet.Consumers;
using StreamNet.Producers;
using Microsoft.Extensions.DependencyInjection;
using StreamNet.Topic;

namespace StreamNet
{
    public static class Startup
    {
        public static void AddConsumer<TConsumer, TEvent>(this IServiceCollection services) where TConsumer : Consumer<TEvent> => services.AddHostedService<TConsumer>();

        public static void AddProducer(this IServiceCollection services) => services.AddTransient<IPublisher, Publisher>();

        public static void AddTopicManagement(this IServiceCollection services) => services.AddTransient<ITopicManagement, TopicManagement>();
    }
}

using StreamNet.Consumers;
using StreamNet.Producers;
using Microsoft.Extensions.DependencyInjection;
using StreamNet.UnitTestingHelpers;

namespace StreamNet
{
    public static class Startup
    {
        public static IServiceCollection AddConsumer<TConsumer, TEvent>(this IServiceCollection services)
            where TConsumer : Consumer<TEvent>
        {
            services.AddHostedService<TConsumer>();
            return services;
        }

        public static IServiceCollection AddProducer(this IServiceCollection services)
        {
            services.AddTransient<IPublisher, Publisher>();
            return services;
        }
    }
}
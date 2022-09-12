using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace StreamNet.Topic
{
    internal class TopicManagement : ITopicManagement
    {
        public TopicManagement()
        {
            Settings.GetInstance();
        }

        public async Task CreateTopicAsync(string topicName, short? replicationFactor = null, int? numberOfPartitions = null)
        {
            var topicSpecification = new TopicSpecification();
            topicSpecification.Name = topicName;
            if (replicationFactor.HasValue)
                topicSpecification.ReplicationFactor = replicationFactor.Value;

            if (numberOfPartitions.HasValue)
                topicSpecification.NumPartitions = numberOfPartitions.Value;

            await CreateTopicAsync(topicSpecification);
        }

        public void DeleteTopicAsync(string topicName)
        {
            var generalConfig = Settings.GetInstance();

            var config = new AdminClientConfig
            {
                BootstrapServers = generalConfig._bootstrapServers,
                SecurityProtocol = generalConfig.GetSecurityProtocol(generalConfig._securityProtocol),
                SaslMechanism = generalConfig.GetSaslMechanism(generalConfig._saslMechanism),
                SaslUsername = generalConfig._username,
                SaslPassword = generalConfig._saslPassword
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                adminClient.DeleteTopicsAsync(new[] { topicName }, new DeleteTopicsOptions
                {
                    OperationTimeout = TimeSpan.FromMilliseconds(2000),
                    RequestTimeout = TimeSpan.FromMilliseconds(2000)
                }).Wait();
            }
        }

        public void DeleteTopicsAsync(IEnumerable<string> topicNames)
        {
            var generalConfig = Settings.GetInstance();

            var config = new AdminClientConfig
            {
                BootstrapServers = generalConfig._bootstrapServers,
                SecurityProtocol = generalConfig.GetSecurityProtocol(generalConfig._securityProtocol),
                SaslMechanism = generalConfig.GetSaslMechanism(generalConfig._saslMechanism),
                SaslUsername = generalConfig._username,
                SaslPassword = generalConfig._saslPassword
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                adminClient.DeleteTopicsAsync(topicNames, new DeleteTopicsOptions
                {
                    OperationTimeout = TimeSpan.FromMilliseconds(2000),
                    RequestTimeout = TimeSpan.FromMilliseconds(2000)
                }).Wait();
            }
        }

        public TopicData GetTopicData(string topicName)
        {
            try
            {
                var generalConfig = Settings.GetInstance();

                var config = new AdminClientConfig
                {
                    BootstrapServers = generalConfig._bootstrapServers,
                    SecurityProtocol = generalConfig.GetSecurityProtocol(generalConfig._securityProtocol),
                    SaslMechanism = generalConfig.GetSaslMechanism(generalConfig._saslMechanism),
                    SaslUsername = generalConfig._username,
                    SaslPassword = generalConfig._saslPassword
                };

                using (var adminClient = new AdminClientBuilder(config).Build())
                {
                    var fullDataTopic = Settings.AdminClient.GetMetadata(topicName, TimeSpan.FromMilliseconds(100));
                    return new TopicData(fullDataTopic?.Topics?.Where(x => !x.Error.IsError));
                }
            }
            catch (Exception)
            {
                return new TopicData(Enumerable.Empty<TopicMetadata>());
            }
        }

        public TopicData GetTopicsData()
        {
            var fullDataTopic = Settings.AdminClient.GetMetadata(TimeSpan.FromMilliseconds(100));
            return new TopicData(fullDataTopic.Topics);
        }

        private async Task CreateTopicAsync(TopicSpecification topicSpecification) => await Settings.AdminClient.CreateTopicsAsync(new[] { topicSpecification });
    }
}

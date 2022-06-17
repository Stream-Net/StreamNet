using System.Threading.Tasks;
using Confluent.Kafka.Admin;

namespace StreamNet.Topic
{
    public class TopicBuilder
    {
        private TopicSpecification _topicSpecification = new TopicSpecification();

        public TopicBuilder WithTopicName(string topicName)
        {
            _topicSpecification.Name = topicName;
            return this;
        }

        public TopicBuilder WithReplicationFactor(short replicationFactor)
        {
            _topicSpecification.ReplicationFactor = replicationFactor;
            return this;
        }

        public TopicBuilder WithNumberOfPartitions(int numberOfPartitions)
        {
            _topicSpecification.NumPartitions = numberOfPartitions;
            return this;
        }

        public async Task Build()
        {
            await Settings.AdminClient.CreateTopicsAsync(new[] {_topicSpecification});
        }
    }
}
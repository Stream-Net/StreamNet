using System.Collections.Generic;
using System.Threading.Tasks;

namespace StreamNet.Topic
{
    public interface ITopicManagement
    {
        Task CreateTopicAsync(string topicName, short? replicationFactor = null, int? numberOfPartitions = null);

        void DeleteTopicAsync(string topicName);

        void DeleteTopicsAsync(IEnumerable<string> topicNames);

        TopicData GetTopicData(string topicName);
        
        TopicData GetTopicsData();
    }
}

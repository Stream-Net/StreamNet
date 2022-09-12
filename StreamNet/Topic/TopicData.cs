using Confluent.Kafka;
using System.Collections.Generic;

namespace StreamNet.Topic
{
    public class TopicData
    {
        public TopicData(IEnumerable<TopicMetadata> data) => Data = data;

        public IEnumerable<TopicMetadata> Data { get; private set; }
    }
}

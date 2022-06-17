using System;

namespace StreamNet.Consumers
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TopicNameAttribute : Attribute
    {
        public string TopicName { get; }
        public TopicNameAttribute(string topicName) => TopicName = topicName;
    }
}
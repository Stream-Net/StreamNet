using System;

namespace StreamNet.Consumers
{
    [AttributeUsage(AttributeTargets.Class)]
    public class ConsumerGroupAttribute : Attribute
    {
        public string ConsumerGroup { get; }
        public ConsumerGroupAttribute(string consumerGroup) => ConsumerGroup = consumerGroup;
    }
}
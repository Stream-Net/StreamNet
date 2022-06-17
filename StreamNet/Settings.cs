using System;
using System.Net;
using Confluent.Kafka;
using StreamNet.Extensions;
using Microsoft.Extensions.Configuration;
using StreamNet.UnitTestingHelpers;

// using StreamNet.UnitTestingHelpers;

namespace StreamNet
{
    public class Settings
    {
        private Settings()
        {
            var configuration = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var bootstrapServers = configuration.GetSection("Kafka:BootstrapServers").Value;
            var saslMechanism = configuration.GetSection("Kafka:SaslMechanism").Value;
            var securityProtocol = configuration.GetSection("Kafka:SecurityProtocol").Value;
            var username = configuration.GetSection("Kafka:Username").Value;
            var password = configuration.GetSection("Kafka:Password").Value;
            int.TryParse(configuration.GetSection("Kafka:RetryCount").Value, out var retryCount);
            RetryCount = retryCount;
            int.TryParse(configuration.GetSection("Kafka:TimeToRetryInSeconds").Value, out var timeToRetryInSeconds);
            TimeToRetryInSeconds = timeToRetryInSeconds;
            int.TryParse(configuration.GetSection("Kafka:ConsumerInstances").Value, out var consumerInstances);
            TimeToRetryInSeconds = timeToRetryInSeconds;

            if (bootstrapServers.IsNullOrEmpty())
                if (!UnitTestDetector.IsRunningFromUnitTesting())
                    throw new ArgumentNullException("BootstrapServers is required!");

            if ((username.IsNullOrEmpty() || password.IsNullOrEmpty()))
                if (!UnitTestDetector.IsRunningFromUnitTesting())
                    throw new ArgumentNullException("Username and password is required!");

            var config = new AdminClientConfig
            {
                BootstrapServers = bootstrapServers,
                SecurityProtocol = GetSecurityProtocol(securityProtocol),
                SaslMechanism = GetSaslMechanism(saslMechanism),
                SaslUsername = username,
                SaslPassword = password,
            };

            AdminClient = new AdminClientBuilder(config).Build();
            ProducerConfig = new ProducerConfig
            {
                ClientId = Dns.GetHostName(),
                BootstrapServers = bootstrapServers,
                SecurityProtocol = GetSecurityProtocol(securityProtocol),
                SaslMechanism = GetSaslMechanism(saslMechanism),
                SaslUsername = username,
                SaslPassword = password,
            };

            ConsumerConfig = new ConsumerConfig
            {
                ClientId = Dns.GetHostName(),
                BootstrapServers = bootstrapServers,
                SecurityProtocol = GetSecurityProtocol(securityProtocol),
                SaslMechanism = GetSaslMechanism(saslMechanism),
                SaslUsername = username,
                SaslPassword = password,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        private static Settings _instance;
        private static readonly object _lock = new object();

        public static Settings GetInstance()
        {
            lock (_lock)
                if (_instance == null)
                    _instance = new Settings();
            return _instance;
        }

        private SecurityProtocol GetSecurityProtocol(string securityProtocol)
        {
            return securityProtocol switch
            {
                "Plaintext" => SecurityProtocol.Plaintext,
                "Ssl" => SecurityProtocol.Ssl,
                "SaslPlaintext" => SecurityProtocol.SaslPlaintext,
                "SaslSsl" => SecurityProtocol.SaslSsl,
                _ => throw new ArgumentNullException("Security Protocol is required!")
            };
        }

        private SaslMechanism GetSaslMechanism(string saslMechanism)
        {
            return saslMechanism switch
            {
                "GssApi" => SaslMechanism.Gssapi,
                "Plain" => SaslMechanism.Plain,
                "ScramSha256" => SaslMechanism.ScramSha256,
                "ScramSha512" => SaslMechanism.ScramSha512,
                "OAuthBearer" => SaslMechanism.OAuthBearer,
                _ => throw new ArgumentNullException("Sasl Mechanism is required !")
            };
        }

        public static IAdminClient AdminClient { get; private set; }
        public static ProducerConfig ProducerConfig { get; private set; }
        public static ConsumerConfig ConsumerConfig { get; private set; }
        public static int RetryCount { get; private set; }
        public static int TimeToRetryInSeconds { get; private set; }
        public static int ConsumerInstances { get; private set; }
    }
}
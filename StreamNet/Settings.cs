using System;
using System.IO;
using System.Net;
using Confluent.Kafka;
using StreamNet.Extensions;
using Microsoft.Extensions.Configuration;
using StreamNet.UnitTestingHelpers;

namespace StreamNet
{
    public class Settings
    {
        private Settings()
        {
            var jsonFiles = Directory.EnumerateFiles(".", "*.json", SearchOption.AllDirectories);
            var builder = new ConfigurationBuilder();
            builder.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            builder.AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json",
                optional: true);
            builder.SetBasePath(Environment.CurrentDirectory);
            builder.AddEnvironmentVariables();
            var configuration = builder.Build();

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
            switch (securityProtocol)
            {
                case "PlainText":
                    return SecurityProtocol.Plaintext;
                case "Ssl":
                    return SecurityProtocol.Ssl;
                case "SaslPlaintext":
                    return SecurityProtocol.SaslPlaintext;
                case "SaslSsl":
                    return SecurityProtocol.SaslSsl;
                default:
                    return !UnitTestDetector.IsRunningFromUnitTesting()
                        ? throw new ArgumentNullException("Security Protocol is required!")
                        : (SecurityProtocol) default!;
            }
        }

        private SaslMechanism GetSaslMechanism(string saslMechanism)
        {
            switch (saslMechanism)
            {
                case "GssApi":
                    return SaslMechanism.Gssapi;
                case "Plain":
                    return SaslMechanism.Plain;
                case "ScramSha256":
                    return SaslMechanism.ScramSha256;
                case "ScramSha512":
                    return SaslMechanism.ScramSha512;
                case "OAuthBearer":
                    return SaslMechanism.OAuthBearer;
                default:
                    return !UnitTestDetector.IsRunningFromUnitTesting()
                        ? throw new ArgumentNullException("Sasl Mechanism is required !")
                        : (SaslMechanism) default!;
            }
        }

        public static IAdminClient AdminClient { get; private set; }
        public static ProducerConfig ProducerConfig { get; private set; }
        public static ConsumerConfig ConsumerConfig { get; private set; }
        public static int RetryCount { get; private set; }
        public static int TimeToRetryInSeconds { get; private set; }
    }
}
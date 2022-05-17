using Confluent.Kafka;
using KafkaDemo.ApplicationCore.Extensions;
using Microsoft.Extensions.Configuration;

namespace KafkaConsumer.Configurations
{
    public class ConfigurationContext
    {
        private readonly IConfiguration configuration;

        public ConfigurationContext(IConfiguration configuration)
        {
            this.configuration = configuration;
        }

        public bool IsDevelopment => (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Development") == "Development";

        public ConsumerConfig KafkaConsumerConfig => configuration.Get<ConsumerConfig>("KafkaConsumer");
        public string KafkaTopic => configuration["KafkaTopic"];
    }
}
using Microsoft.Extensions.Configuration;

namespace KafkaDemo.ApplicationCore.Extensions
{
    public static class ConfigurationExtension
    {
        public static T Get<T>(this IConfiguration config, string key) where T : new()
        {
            T instance = new();
            config.GetSection(key).Bind(instance);
            return instance;
        }
    }
}
using KafkaProducer.Configurations;
using KafkaProducer.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace KafkaProducer
{
    public class Startup
    {
        private readonly IServiceCollection services;

        public Startup()
        {
            services = new ServiceCollection();
            ConfigureServices(services);
        }

        public async Task StartAsync(
            bool enabledConcurrent,
            int maxConcurrent)
        {
            var serviceProvider = services.BuildServiceProvider();
            var service = serviceProvider.GetService<DemoProducerTask>();

            if (enabledConcurrent)
            {
                await service.RunConcurrentAsync(maxConcurrent);
            }
            else
            {
                await service.RunAsync();
            }
        }

        public void ConfigureServices(IServiceCollection services)
        {
            var config = BuildConfiguration();
            services.AddSingleton(config);

            services.AddTransient<ConfigurationContext>();

            // add services here
            services.AddScoped<DemoProducerTask>();

            // set Globalization
            Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
        }

        private IConfiguration BuildConfiguration()
        {
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Development";

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                .AddJsonFile($"appsettings.{environmentName}.json", optional: true);

            return builder.Build();
        }
    }
}
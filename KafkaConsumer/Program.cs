using KafkaProducer;

namespace KafkaConsumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var startup = new Startup();
            startup.StartAsync().Wait();
        }
    }
}
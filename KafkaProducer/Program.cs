namespace KafkaProducer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.Write("Enable Concurrent (y/n)?: ");
            var input = Console.ReadLine();
            bool enabledConcurrent = input.Trim().ToLower() == "y";
            int maxConcurrent = 1;
            if (enabledConcurrent)
            {
                Console.Write("Max Concurrent (1 - 20x of cpu cores based on use case)?: ");

                input = Console.ReadLine();
                maxConcurrent = Convert.ToInt32(input);
            }

            var startup = new Startup();
            startup.StartAsync(enabledConcurrent, maxConcurrent).Wait();
        }
    }
}
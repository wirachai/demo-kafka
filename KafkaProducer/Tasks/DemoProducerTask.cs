using Confluent.Kafka;
using KafkaDemo.ApplicationCore.Utility;
using KafkaProducer.Configurations;
using KafkaProducer.Models;
using Newtonsoft.Json;

namespace KafkaProducer.Tasks
{
    internal class DemoProducerTask
    {
        private readonly IProducer<Null, string> kafkaProducer;
        private readonly string kafkaTopic;
        private readonly string machineName;
        private Progress totalSent = new Progress();

        public DemoProducerTask(ConfigurationContext configuration)
        {
            kafkaProducer = new ProducerBuilder<Null, string>(configuration.KafkaProducerConfig).Build();
            kafkaTopic = configuration.KafkaTopic;
            machineName = Environment.MachineName;
        }

        public async Task RunAsync()
        {
            var demoPeriod = TimeSpan.FromSeconds(30);
            var cts = new CancellationTokenSource(demoPeriod);

            WriteLog("Started to produce message to kafka.");
            Task progressTask = CreateProgressTask(cts.Token);
            try
            {
                while (true)
                {
                    cts.Token.ThrowIfCancellationRequested();

                    await SendMessage();
                }
            }
            catch (OperationCanceledException)
            {
                WriteLog("Operation Canceled.");
            }
            catch (Exception ex)
            {
                WriteLog(ex.ToString());
            }
            finally
            {
                cts.Cancel();
                await progressTask;

                WriteLog($"Produced {totalSent} messages to kafka.");
                WriteLog("Stopped processing.");
            }
        }

        public async Task RunConcurrentAsync(int maxConcurrent)
        {
            var demoPeriod = TimeSpan.FromSeconds(30);
            var cts = new CancellationTokenSource(demoPeriod);

            List<Task> tasks = new List<Task>();

            WriteLog("Started to produce message to kafka.");
            WriteLog($"Max Concurrent = {maxConcurrent} by {Environment.ProcessorCount} CPU cores");
            Task progressTask = CreateProgressTask(cts.Token);
            try
            {
                while (true)
                {
                    cts.Token.ThrowIfCancellationRequested();

                    var task = SendMessage();
                    tasks.Add(task);

                    while (tasks.Count >= maxConcurrent)
                    {
                        var finishedTask = await Task.WhenAny(tasks);
                        tasks.Remove(finishedTask);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                WriteLog("Operation Canceled.");
            }
            catch (Exception ex)
            {
                WriteLog(ex.ToString());
            }
            finally
            {
                cts.Cancel();
                await progressTask;

                // make sure all tasks are finished
                await Task.WhenAll(tasks);

                WriteLog($"Produced {totalSent} messages to kafka.");
                WriteLog("Stopped processing.");
            }
        }

        private async Task SendMessage()
        {
            DemoMessage data = new DemoMessage
            {
                Timestamp = DateTime.Now,
                MachineName = machineName,  // for demo purpose
                SomeText = $"Hello {totalSent.Current + 1:#,##0}"
            };
            string message = JsonConvert.SerializeObject(data);
            await kafkaProducer.ProduceAsync(kafkaTopic, new Message<Null, string> { Value = message });

            totalSent.Increase();
        }

        private Task CreateProgressTask(CancellationToken token)
        {
            var logPeriod = TimeSpan.FromSeconds(1);
            var startTime = DateTime.Now;
            return Task.Run(async () =>
            {
                while (token.IsCancellationRequested == false)
                {
                    await Task.Delay(logPeriod);

                    var tps = totalSent.Current / (DateTime.Now - startTime).TotalSeconds;
                    WriteLog($"Produced {totalSent} messages to kafka. ({tps:0.00} message per second)");
                }
            });
        }

        // TODO: implement some logging like Nlog
        private static void WriteLog(string message)
        {
            Console.WriteLine($"{DateTime.Now.ToString("s")}: {message}");
        }
    }
}
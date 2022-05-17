using Confluent.Kafka;
using KafkaConsumer.Configurations;
using KafkaDemo.ApplicationCore.Models;
using KafkaDemo.ApplicationCore.Utility;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace KafkaConsumer.Tasks
{
    internal class DemoConsumerTask
    {
        private readonly ConfigurationContext configuration;
        private readonly ConcurrentQueue<DemoMessage> localQueues = new ConcurrentQueue<DemoMessage>();
        private Progress totalProcessed = new Progress();

        public DemoConsumerTask(ConfigurationContext configuration)
        {
            this.configuration = configuration;
        }

        public async Task RunAsync()
        {
            var cts = new CancellationTokenSource();

            WriteLog("Started to produce message to kafka.");
            WriteLog("To terminal this app, press Ctrl+C.");

            Task progressTask = CreateProgressTask(cts.Token);
            try
            {
                Console.CancelKeyPress += (_, e) =>
                {
                    WriteLog(">> Cancel Key Pressed, stopping process ...");
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                ConsumeKafkaMessage(cts.Token);
            }
            catch (Exception ex)
            {
                WriteLog(ex.ToString());
            }
            finally
            {
                cts.Cancel();
                await progressTask;

                WriteLog($"Produced {totalProcessed} messages to kafka.");
                WriteLog("Stopped processing.");
            }
        }

        private void ConsumeKafkaMessage(CancellationToken cancellationToken)
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(configuration.KafkaConsumerConfig)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    WriteLog(
                        "Partitions assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
                    WriteLog(
                        "Partitions revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                        "]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    WriteLog($"Partitions were lost: [{string.Join(", ", partitions)}]");
                })
                .Build())
            {
                WriteLog($"Subscribe {configuration.KafkaTopic} topic");
                consumer.Subscribe(configuration.KafkaTopic);

                try
                {
                    while (true)
                    {
                        try
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            var consumeResult = consumer.Consume(cancellationToken);
                            if (consumeResult.IsPartitionEOF)
                            {
                                WriteLog($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                                continue;
                            }

                            // process message
                            var message = JsonConvert.DeserializeObject<DemoMessage>(consumeResult.Message.Value);
                            WriteLog($"{message.SomeText} from {message.MachineName} @partition [{consumeResult.Partition.Value}]");

                            // update progress
                            totalProcessed.Increase();
                        }
                        catch (ConsumeException e)
                        {
                            WriteLog($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    WriteLog(">> Kafka consuming operation canceled");
                }
                catch (Exception ex)
                {
                    WriteLog(ex.ToString());
                }
                finally
                {
                    consumer.Close();
                    WriteLog($">> Stop consuming kafka message");
                }
            }
        }

        private Task CreateProgressTask(CancellationToken token)
        {
            var logPeriod = TimeSpan.FromSeconds(1);

            int lastProgress = 0;
            DateTime lastUpdate = DateTime.Now;
            return Task.Run(async () =>
            {
                while (token.IsCancellationRequested == false)
                {
                    await Task.Delay(logPeriod);

                    int progress = totalProcessed.Current - lastProgress;
                    TimeSpan elapsedTime = DateTime.Now - lastUpdate;
                    var tps = progress / elapsedTime.TotalSeconds;
                    WriteLog($"Consumed {totalProcessed} messages from kafka. ({tps:0.00} message per second)");

                    lastProgress = totalProcessed.Current;
                    lastUpdate = DateTime.Now;
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
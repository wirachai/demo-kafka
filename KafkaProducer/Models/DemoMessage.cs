namespace KafkaProducer.Models
{
    internal class DemoMessage
    {
        public DateTime Timestamp { get; set; }
        public string MachineName { get; set; }
        public string SomeText { get; set; }
    }
}
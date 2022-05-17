namespace KafkaDemo.ApplicationCore.Utility
{
    // Thread-safe progressing to prevent incorrect number on concurrency updating progress
    public class Progress
    {
        private int current = 0;
        public int Current => current;

        public void Reset()
        {
            current = 0;
        }

        public int Increase()
        {
            // calling current++ will produce a bug on concurrency process (e.g. async)
            return Interlocked.Increment(ref current);
        }

        public override string ToString()
        {
            return current.ToString("#,##0");
        }
    }
}
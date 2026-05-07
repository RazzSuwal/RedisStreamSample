using StackExchange.Redis;

namespace RedisStreamDemo
{
    public class Producer
    {
        private readonly IDatabase _db;
        private readonly string _streamName;

        public Producer(IDatabase db, string streamName)
        {
            _db = db;
            _streamName = streamName;
        }

        // Produce a single message with key-value fields
        public async Task ProduceAsync(Dictionary<string, string> fields)
        {
            var entries = fields
                .Select(f => new NameValueEntry(f.Key, f.Value))
                .ToArray();

            var messageId = await _db.StreamAddAsync(_streamName, entries);
            Console.WriteLine($"[Producer] Sent to '{_streamName}' | ID: {messageId}");
        }

        // Produce messages continuously
        public async Task ProduceContinuouslyAsync(CancellationToken token)
        {
            Console.WriteLine($"[Producer] Starting on stream: {_streamName}");
            var random = new Random();
            int count = 0;

            while (!token.IsCancellationRequested)
            {
                try
                {
                    var fields = new Dictionary<string, string>
                    {
                        { "temperature", random.Next(20, 100).ToString() },
                        { "humidity",    random.Next(30, 90).ToString()  },
                        { "device",      $"device-{random.Next(1, 5)}"   },
                        { "timestamp",   DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString() }
                    };

                    await ProduceAsync(fields);

                    count++;
                    Console.WriteLine($"[Producer] Total messages sent: {count}");

                    // Wait before sending next message
                    await Task.Delay(10000, token);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("[Producer] Stopped.");
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Producer] Error: {ex.Message}");
                    await Task.Delay(2000, token); 
                }
            }
        }
    }
}
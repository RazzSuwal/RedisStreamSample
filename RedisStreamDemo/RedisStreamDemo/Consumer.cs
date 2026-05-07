using StackExchange.Redis;

namespace RedisStreamDemo
{
    public class Consumer
    {
        private readonly IDatabase _db;
        private readonly string _streamName;
        private readonly string _groupName;
        private readonly string _consumerName;

        public Consumer(IDatabase db, string streamName, string groupName)
        {
            _db = db;
            _streamName = streamName;
            _groupName = groupName;
            _consumerName = $"consumer-{Guid.NewGuid().ToString()[..8]}";
        }

        public async Task InitializeAsync()
        {
            bool keyExists = await _db.KeyExistsAsync(_streamName);
            bool groupExists = keyExists &&
                (await _db.StreamGroupInfoAsync(_streamName))
                .Any(x => x.Name == _groupName);

            if (!groupExists)
            {
                await _db.StreamCreateConsumerGroupAsync(_streamName, _groupName,"0-0", true);
                Console.WriteLine($"[Consumer] Created group '{_groupName}' on stream '{_streamName}'");
            }
            else
            {
                Console.WriteLine($"[Consumer] Group '{_groupName}' already exists on stream '{_streamName}'");
            }
        }

        public async Task ConsumeAsync(CancellationToken token)
        {
            Console.WriteLine($"[Consumer] '{_consumerName}' listening on stream: '{_streamName}'");

            while (!token.IsCancellationRequested)
            {
                try
                {
                    await RecoverPendingAsync(token);

                    //Raw XREADGROUP with BLOCK 30000ms (30 seconds)
                    var result = await _db.ExecuteAsync(
                        "XREADGROUP",
                        "GROUP", _groupName, _consumerName,
                        "BLOCK", "30000",   // block up to 30s, wakes instantly on message
                        "COUNT", "10",
                        "STREAMS", _streamName,
                        ">"
                    );

                    if (result.IsNull)
                        continue;

                    // Parse the nested result: [[streamName, [[id, [k,v,k,v...]], ...]]]
                    var streamEntries = (RedisResult[]?)result;
                    if (streamEntries is null || streamEntries.Length == 0)
                        continue;

                    var streamData = (RedisResult[]?)streamEntries[0];
                    var messageList = (RedisResult[]?)streamData?[1];

                    foreach (var entry in messageList!)
                    {
                        var parts = (RedisResult[]?)entry;
                        var messageId = (string?)parts?[0];
                        var fields = (RedisResult[]?)parts?[1];

                        try
                        {
                            Console.WriteLine($"\n[Consumer] ID: {messageId}");
                            for (int i = 0; i < fields?.Length - 1; i += 2)
                            {
                                Console.WriteLine($"[Consumer] {(string?)fields?[i],-15}: {(string?)fields?[i + 1]}");
                            }

                            // ACK
                            await _db.StreamAcknowledgeAsync(
                                _streamName, _groupName, messageId);
                            Console.WriteLine($"[Consumer] ✅ Acknowledged: {messageId}");

                            // Delete
                            await _db.StreamDeleteAsync(
                                _streamName, new RedisValue[] { messageId });
                            Console.WriteLine($"[Consumer] 🗑️  Deleted: {messageId}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Consumer] ❌ Failed: {messageId} — {ex.Message}");
                        }
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Consumer] Error: {ex.Message}");
                    await Task.Delay(2000, token);
                }
            }
        }

        private async Task RecoverPendingAsync(CancellationToken token)
        {
            Console.WriteLine($"[Consumer] Checking PEL for stuck messages...");

            while (!token.IsCancellationRequested)
            {
                // XAUTOCLAIM: atomically claim messages stuck in PEL for > 30 seconds
                // Means: "give me messages that ANY dead consumer was holding for 30s+"
                var result = await _db.ExecuteAsync(
                    "XAUTOCLAIM",
                    _streamName,
                    _groupName,
                    _consumerName,          // claim ownership under THIS consumer
                    "30000",                // min-idle-time: 30 seconds
                    "0-0",                  // start from the beginning of PEL
                    "COUNT", "10"
                );

                if (result.IsNull)
                {
                    Console.WriteLine($"[Consumer] No pending messages found.");
                    break;
                }

                var parts = (RedisResult[]?)result;
                var nextStartId = (string?)parts?[0];   // cursor for next batch
                var messageList = (RedisResult[]?)parts?[1];

                if (messageList?.Length == 0)
                {
                    Console.WriteLine($"[Consumer] PEL is clean — no stuck messages.");
                    break;
                }

                Console.WriteLine($"[Consumer] Found {messageList?.Length} stuck message(s) — reprocessing...");

                foreach (var entry in messageList!)
                {
                    var entryParts = (RedisResult[]?)entry;
                    var messageId = (string?)entryParts?[0];
                    var fields = (RedisResult[]?)entryParts?[1];

                    try
                    {
                        Console.WriteLine($"[Consumer] ♻️  Recovering: {messageId}");

                        for (int i = 0; i < fields?.Length - 1; i += 2)
                            Console.WriteLine($"[Consumer] {(string?)fields?[i],-15}: {(string?)fields?[i + 1]}");

                        await _db.StreamAcknowledgeAsync(_streamName, _groupName, messageId);
                        Console.WriteLine($"[Consumer] ✅ Re-acknowledged: {messageId}");

                        await _db.StreamDeleteAsync(_streamName, new RedisValue[] { messageId });
                        Console.WriteLine($"[Consumer] 🗑️  Deleted: {messageId}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Consumer] ❌ Recovery failed for {messageId}: {ex.Message}");
                        // Still won't ACK — will be retried on next pod restart
                    }
                }

                // If cursor is "0-0" we've processed all pending messages
                if (nextStartId == "0-0")
                    break;
            }
        }
    }
}
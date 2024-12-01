using Confluent.Kafka;

namespace Kafka;

internal class KafkaService
{
    internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-1-group-2",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(topicName);

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                Console.WriteLine($"Gelen mesaj: {consumeResult.Message.Value}");
            }

            await Task.Delay(500);
        }

    }
}
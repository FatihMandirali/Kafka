using System.Text;
using Confluent.Kafka;
using Kafka.Consumer.Events;

namespace Kafka;

internal class KafkaService
{
    internal async Task ConsumeSimpleMessageWithNullKey(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-1-group-1",
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
    
    internal async Task ConsumeSimpleMessageWithIntKey(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-2-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<int, string>(config).Build();
        consumer.Subscribe(topicName);

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                Console.WriteLine($"Key: {consumeResult.Message.Key} - Gelen mesaj: {consumeResult.Message.Value}");
            }

            await Task.Delay(500);
        }

    }
    
    internal async Task ConsumeComplexMessageWithIntKey(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-2-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
            .Build();
        consumer.Subscribe(topicName);

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                Console.WriteLine($"Key: {consumeResult.Message.Key} - Gelen mesaj: {consumeResult.Message.Value.UserId}-{consumeResult.Message.Value.OrderCode}-{consumeResult.Message.Value.TotalPrice}");
            }

            await Task.Delay(500);
        }

    }
    
    internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-2-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<int, OrderCreatedEvent>(config)
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>())
            .Build();
        consumer.Subscribe(topicName);

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                //consumeResult.Message.Timestamp //Mesajın producer'dan çıktığı tarih.
                var correlationId = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("correlation_id"));
                var version = Encoding.UTF8.GetString(consumeResult.Message.Headers.GetLastBytes("version"));
                Console.WriteLine($"Key: {consumeResult.Message.Key} - Gelen mesaj: {consumeResult.Message.Value.UserId}-{consumeResult.Message.Value.OrderCode}-{consumeResult.Message.Value.TotalPrice}");
            }

            await Task.Delay(500);
        }

    }
    
    //Sadece istediğimiz parititondan veri okuma
    internal async Task ConsumerMessageFromSpecificPartition(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-2-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        
        consumer.Assign(new List<TopicPartition>{new TopicPartition(topicName,new Partition(1))});

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                Console.WriteLine($"Key: {consumeResult.Message.Key} - Message {consumeResult.Message.Value}");
            }

            await Task.Delay(500);
        }

    }
    
    //İstediğimiz partitionun istediğimizi offsetinden veri okumamızı sağlar.....
    internal async Task ConsumerMessageFromSpecificPartitionOffset(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-2-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        
        consumer.Assign(new List<TopicPartitionOffset>{new TopicPartitionOffset(topicName,new Partition(1),2)});

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                Console.WriteLine($"Key: {consumeResult.Message.Key} - Message {consumeResult.Message.Value}");
            }

            await Task.Delay(500);
        }

    }
    
    internal async Task ConsumerMessageWithAcknowledge(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:9094",
            GroupId = "use-case-2-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(topicName);

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                try
                {
                    Console.WriteLine($"Key: {consumeResult.Message.Key} - Message {consumeResult.Message.Value}");
                    consumer.Commit(consumeResult);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }

            await Task.Delay(500);
        }

    }
    
    internal async Task ConsumerMessageToCluster(string topicName)
    {
        var config = new ConsumerConfig()
        {
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002",
            GroupId = "use-case-3-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var consumer = new ConsumerBuilder<Null, string>(config).Build();
        consumer.Subscribe(topicName);

        while (true)
        {
            var consumeResult = consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                try
                {
                    Console.WriteLine($"Key: {consumeResult.Message.Key} - Message {consumeResult.Message.Value}");
                    consumer.Commit(consumeResult);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }

            await Task.Delay(500);
        }

    }
}
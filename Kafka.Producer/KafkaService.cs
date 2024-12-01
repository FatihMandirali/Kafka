using System.Text;
using System.Text.Unicode;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kafka.Producer.Events;

namespace Kafka.Producer;

internal class KafkaService
{
    internal async Task CreateTopicAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = "localhost:9094"
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification(){Name = topicName,NumPartitions = 3,ReplicationFactor = 1}
            });
            
            Console.WriteLine($"Topic oluştu : {topicName}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task CreateTopicWithClusterAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            //**sadece ayakta olan bir port yazsak dahi cluster birbirini gördüğü için çalışır
            //En uygun olan tüm broker'ları yazmak çünkü ayakta değilse 1 tane yazdığımız hiç çalışmaz.
            //Fakat hepsini yazarsak bir tanesi dahi ayakta ise yeterli olur.
            //BootstrapServers = "localhost:7000"
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002"
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification(){Name = topicName,NumPartitions = 3,ReplicationFactor = 3}
            });
            
            Console.WriteLine($"Topic oluştu : {topicName}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task CreateTopicRetryWithClusterAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            //**sadece ayakta olan bir port yazsak dahi cluster birbirini gördüğü için çalışır
            //En uygun olan tüm broker'ları yazmak çünkü ayakta değilse 1 tane yazdığımız hiç çalışmaz.
            //Fakat hepsini yazarsak bir tanesi dahi ayakta ise yeterli olur.
            //BootstrapServers = "localhost:7000"
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002"
        }).Build();

        try
        {
            var configs = new Dictionary<string, string>()
            {
                {"min.insync.replicas","3"}
            };
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification()
                {
                    Name = topicName,NumPartitions = 3,ReplicationFactor = 3,Configs = configs
                }
            });
            
            Console.WriteLine($"Topic oluştu : {topicName}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task CreateTopicWithRetentionAsync(string topicName)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = "localhost:9094"
        }).Build();

        try
        {
            var configs = new Dictionary<string, string>()
            {
                //https://docs.confluent.io/platform/current/installation/configuration/topic-configs.html
                //-1 dersek ömür boyu silinmez mesajlar kafkadan
                //{"retention.ms","-1"}
                //{"retention.ms",TimeSpan.FromDays(3).Milliseconds.ToString()} //3gün ömür verdik kafakda saklaması için
                {"retention.bytes","-1"} //byte cinsinden verileri tutacağı kısıtı belirleriz. -1 sınırsız boyuta kadar tutar.
            };
            await adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification()
                {
                    Name = topicName,NumPartitions = 3,ReplicationFactor = 1,
                    Configs = configs
                }
            });
            
            Console.WriteLine($"Topic oluştu : {topicName}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
    internal async Task SendSimpleMessageWithNullKey(string topicName)
    {
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        foreach (var item in Enumerable.Range(1,10))
        {
            var message = new Message<Null, string>()
            {
                Value = $"Message(use case - 1) - {item}"
            };
            
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(400);
        }
    }
    internal async Task SendSimpleMessageWithIntKey(string topicName)
    {
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
        using var producer = new ProducerBuilder<int, string>(config).Build();
        foreach (var item in Enumerable.Range(1,100))
        {
            var message = new Message<int, string>()
            {
                Value = $"Message(use case - 1) - {item}",
                Key = item
            };
            
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    internal async Task SendComplexMessageWithIntKey(string topicName)
    {
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .Build();
        foreach (var item in Enumerable.Range(1,100))
        {
            var orderCreateEvent = new OrderCreatedEvent
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item*100,
                UserId = 123
            };
            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreateEvent,
                Key = item
            };
            
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    internal async Task SendComplexMessageWithIntKeyAndHeader(string topicName)
    {
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .Build();
        foreach (var item in Enumerable.Range(1,10))
        {
            var orderCreateEvent = new OrderCreatedEvent
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item*100,
                UserId = 123
            };
            
            var correlationId = Encoding.UTF8.GetBytes("123");
            var version = Encoding.UTF8.GetBytes("v1");
            var header = new Headers
            {
                { "correlation_id", correlationId },
                { "version", version }
            };
            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreateEvent,
                Key = item,
                Headers = header
            };
            
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    internal async Task SendMessageTimeStamp(string topicName)
    {
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
        using var producer = new ProducerBuilder<int, OrderCreatedEvent>(config)
            .SetValueSerializer(new CustomValueSerializer<OrderCreatedEvent>())
            .Build();
        foreach (var item in Enumerable.Range(1,10))
        {
            var orderCreateEvent = new OrderCreatedEvent
            {
                OrderCode = Guid.NewGuid().ToString(),
                TotalPrice = item*100,
                UserId = 123
            };
            
            var correlationId = Encoding.UTF8.GetBytes("123");
            var version = Encoding.UTF8.GetBytes("v1");
            var header = new Headers
            {
                { "correlation_id", correlationId },
                { "version", version }
            };
            var message = new Message<int, OrderCreatedEvent>()
            {
                Value = orderCreateEvent,
                Key = item,
                Headers = header,
                //Timestamp = new Timestamp(new DateTime(2012,12,12))
            };
            
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    internal async Task SendMessageToSpecificPartition(string topicName)
    {
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094" };
        using var producer = new ProducerBuilder<Null, string>(config)
            .Build();
        foreach (var item in Enumerable.Range(1,10))
        {
            var message = new Message<Null, string>()
            {
                Value = $"Message(use case - 1) - {item}"
            };
            //Sadece istediğimiz partitiona mesaj gönderme
            var topicPartition = new TopicPartition(topicName, new Partition(1));
            
            var result = await producer.ProduceAsync(topicPartition,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    
    internal async Task SendMessageWithAcknowledge(string topicName)
    {
        //Acks değerini belirtiyoruz..
        var config = new ProducerConfig() { BootstrapServers = "localhost:9094", Acks = Acks.None};
        using var producer = new ProducerBuilder<Null, string>(config)
            .Build();
        foreach (var item in Enumerable.Range(1,10))
        {
            var message = new Message<Null, string>()
            {
                Value = $"Message(use case - 1) - {item}"
            };
            //Sadece istediğimiz partitiona mesaj gönderme
            var topicPartition = new TopicPartition(topicName, new Partition(1));
            
            var result = await producer.ProduceAsync(topicPartition,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    
    internal async Task SendMessageToCluster(string topicName)
    {
        //**sadece ayakta olan bir port yazsak dahi cluster birbirini gördüğü için çalışır
        //En uygun olan tüm broker'ları yazmak çünkü ayakta değilse 1 tane yazdığımız hiç çalışmaz.
        //Fakat hepsini yazarsak bir tanesi dahi ayakta ise yeterli olur.
        //BootstrapServers = "localhost:7000"
        var config = new ProducerConfig() { BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", Acks = Acks.All};
        using var producer = new ProducerBuilder<Null, string>(config)
            .Build();
        foreach (var item in Enumerable.Range(1,10))
        {
            var message = new Message<Null, string>()
            {
                Value = $"Message(use case - 1) - {item}"
            };
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
            
            Console.WriteLine("-----------");
            await Task.Delay(100);
        }
    }
    internal async Task SendMessageWithRetryToCluster(string topicName)
    {
        //**sadece ayakta olan bir port yazsak dahi cluster birbirini gördüğü için çalışır
        //En uygun olan tüm broker'ları yazmak çünkü ayakta değilse 1 tane yazdığımız hiç çalışmaz.
        //Fakat hepsini yazarsak bir tanesi dahi ayakta ise yeterli olur.
        //BootstrapServers = "localhost:7000"
        var config = new ProducerConfig()
        {
            //MessageTimeoutMs, belirledik be acks'yi all yaptığımız için lider partitiona ve replicalara mesajı yazmaya çalışır. eğerki herhangi bir replica parititona erişemez ise hata fırlatır.
            //Eğerki Acks'yi al dışında bir şey belirlersek tümüne göndermek zorunda olmayacağı için mesajı ayakta olanlara gönderir.
            BootstrapServers = "localhost:7000,localhost:7001,localhost:7002", Acks = Acks.All,
            //MessageTimeoutMs = 10000,  //Messajı göndermeye çalışır 10sn içinde gönderemezse hata fırlatır
            MessageSendMaxRetries = 3, //Mesajı 10 kere göndermeye çalışır gönderemezse hata fırlatır
            RetryBackoffMs = 2000 , //MessageSendMaxRetries 10 dersek her biri arasına 2 sn koyarak dener.
            RetryBackoffMaxMs = 2000 //Default RetryBackoffMs 1000 iken RetryBackoffMaxMs ile bunu değiştirebiliyoruz.
        };
        using var producer = new ProducerBuilder<Null, string>(config)
            .Build();
        
            var message = new Message<Null, string>()
            {
                Value = $"Message 1"
            };
            var result = await producer.ProduceAsync(topicName,message);

            foreach (var propertyInfo in result.GetType().GetProperties())
            {
                Console.WriteLine($"{propertyInfo.Name} : {propertyInfo.GetValue(result)}");
            }
        
    }
}
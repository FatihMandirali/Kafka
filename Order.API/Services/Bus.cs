using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared.Events;

namespace Order.API.Services;

public class Bus : IBus
{
    private readonly ProducerConfig config;
    private readonly IConfiguration _configuration;
    public Bus(IConfiguration configuration)
    {
        _configuration = configuration;
        config = new ProducerConfig()
        {
            //MessageTimeoutMs, belirledik be acks'yi all yaptığımız için lider partitiona ve replicalara mesajı yazmaya çalışır. eğerki herhangi bir replica parititona erişemez ise hata fırlatır.
            //Eğerki Acks'yi al dışında bir şey belirlersek tümüne göndermek zorunda olmayacağı için mesajı ayakta olanlara gönderir.
            BootstrapServers = _configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"],
            Acks = Acks.All,
            MessageTimeoutMs = 6000,  //Messajı göndermeye çalışır 10sn içinde gönderemezse hata fırlatır
            //MessageSendMaxRetries = 3, //Mesajı 10 kere göndermeye çalışır gönderemezse hata fırlatır
            //RetryBackoffMs = 2000 , //MessageSendMaxRetries 10 dersek her biri arasına 2 sn koyarak dener.
            //RetryBackoffMaxMs = 2000 //Default RetryBackoffMs 1000 iken RetryBackoffMaxMs ile bunu değiştirebiliyoruz.
            AllowAutoCreateTopics = true
        };
    }

    public async Task<bool> Publish<T1, T2>(T1 key, T2 value, string topicOrQueueName)
    {
        using var producer = new ProducerBuilder<T1, T2>(config)
            .SetValueSerializer(new CustomValueSerializer<T2>())
            .SetKeySerializer(new CustomValueSerializer<T1>())
            .Build();

        var message = new Message<T1, T2>()
        {
            Key = key,
            Value = value
        };

        var result = await producer.ProduceAsync(topicOrQueueName, message);
        return result.Status == PersistenceStatus.Persisted;
    }

    public async Task CreateTopicOrQueue(List<string> topicOrQueueNameList)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig()
        {
            BootstrapServers = _configuration.GetSection("BusSettings").GetSection("Kafka")["BootstrapServers"]
        }).Build();

        try
        {

            foreach (var topicOrQueueName in topicOrQueueNameList)
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification(){Name = topicOrQueueName,NumPartitions = 3,ReplicationFactor = 1}
                });
            
                Console.WriteLine($"Topic oluştu : {topicOrQueueName}"); 
            }
            
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
}
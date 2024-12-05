using Confluent.Kafka;
using Shared.Events;
using Stock.API.Services;

namespace Stock.API;

public class OrderCreatedEventConsumerBackgroundService(IBus bus) :BackgroundService
{
    private IConsumer<string, OrderCreatedEvent> _consumer;
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _consumer = new ConsumerBuilder<string, OrderCreatedEvent>(bus.GetConsumerConfig(BusConsts.OrderCreatedEventGroupId))
            .SetValueDeserializer(new CustomValueDeserializer<OrderCreatedEvent>()).Build();
        _consumer.Subscribe(BusConsts.OrderCreatedEventTopicName);
        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        

        while (!stoppingToken.IsCancellationRequested)
        {
            var consumeResult = _consumer.Consume(5000);
            //5 sn bekler kafkadan mesaj varsa alır yok ise alt satıra gider ve while döngünün başına döner...
            if (consumeResult != null)
            {
                try
                {
                    var orderCreatedEvent = consumeResult.Message.Value;

                    Console.WriteLine($"Key: {consumeResult.Message.Key} - Message {consumeResult.Message.Value}");
                    _consumer.Commit(consumeResult);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }

            await Task.Delay(500,stoppingToken);
        }
    }
}
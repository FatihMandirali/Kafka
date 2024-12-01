namespace Kafka.Producer.Events;

internal record OrderCreatedEvent
{
    public string OrderCode { get; init; }
    public decimal TotalPrice { get; init; }
    public int UserId { get; init; }
}
namespace Shared.Events;

public record OrderCreatedEvent(string OrderCode,string UserId,decimal TotalPrice);
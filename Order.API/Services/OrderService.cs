using Order.API.Dtos;
using Shared.Events;

namespace Order.API.Services;

public class OrderService
{
    private readonly IBus _bus;

    public OrderService(IBus bus)
    {
        _bus = bus;
    }

    public async Task<bool> Create(OrderCreatedRequestDto request)
    {
        var orderCode = Guid.NewGuid().ToString();
        var orderCreatedEvent = new OrderCreatedEvent(orderCode,request.UserId,request.TotalPrice);
        return await _bus.Publish(orderCode, orderCreatedEvent ,BusConsts.OrderCreatedEventTopicName);
    }
}
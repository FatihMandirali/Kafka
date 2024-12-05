using Shared.Events;

namespace Order.API.Services;

public static class BusExt
{
    public static async Task CreateTopicsOrQueues(this WebApplication application)
    {
        using (var scope = application.Services.CreateScope())
        {
            var ibus = scope.ServiceProvider.GetRequiredService<IBus>();
            await ibus.CreateTopicOrQueue([BusConsts.OrderCreatedEventTopicName]);
        }
    }
}
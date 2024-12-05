using Microsoft.AspNetCore.Mvc;
using Order.API.Dtos;
using Order.API.Services;

namespace Order.API.Controllers;

[Route("api/[controller]")]
[ApiController]
public class OrderController:ControllerBase
{
    private readonly OrderService _orderService;

    public OrderController(OrderService orderService)
    {
        _orderService = orderService;
    }

    [HttpPost]
    public async Task<IActionResult> Create(OrderCreatedRequestDto request)
    {
        return Ok(await _orderService.Create(request));
    }
}
namespace Order.API.Dtos;

public record OrderCreatedRequestDto(string UserId,decimal TotalPrice);
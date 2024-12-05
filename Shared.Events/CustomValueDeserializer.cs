using System.Text.Json;
using System.Text.Unicode;
using Confluent.Kafka;

namespace Shared.Events;

public class CustomValueDeserializer<T> : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        return JsonSerializer.Deserialize<T>(data);
    }
}
using Kafka;

Console.WriteLine("Kafka Consumer 2");

var topicName = "use-case-1.1-topic";

var kafkaService = new KafkaService();
kafkaService.ConsumeSimpleMessageWithNullKey(topicName);

Console.ReadLine();
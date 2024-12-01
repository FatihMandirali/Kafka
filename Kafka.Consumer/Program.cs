using Kafka;

Console.WriteLine("Kafka Consumer 1");

var topicName = "use-case-6-topic";

var kafkaService = new KafkaService();
kafkaService.ConsumerMessageToCluster(topicName);

Console.ReadLine();

using Kafka.Producer;

Console.WriteLine("Kafka Producer");

var kafkaService = new KafkaService();
string topicName = "retry-topic";
//await kafkaService.CreateTopicRetryWithClusterAsync(topicName);
await kafkaService.SendMessageWithRetryToCluster(topicName);

//await kafkaService.SendSimpleMessageWithNullKey(topicName);

//await kafkaService.SendSimpleMessageWithIntKey(topicName);

//await kafkaService.SendComplexMessageWithIntKey(topicName);

//await kafkaService.SendMessageToSpecificPartition(topicName);

Console.WriteLine("Mesajlar gönderilmiştir... :)");



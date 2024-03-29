package com.sbk.kafka.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		String bootstrapServers = "127.0.0.1:9092";
		String topicName = "first_topic";

		// create consumer configs
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// assign and seek are mostly used replay data or fetch a specific message

		// assign
		TopicPartition partitionToReadFrom = new TopicPartition(topicName, 0);
		long offsetToReadFrom = 15L;
		consumer.assign(Collections.singletonList(partitionToReadFrom));

		// seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);

		int numberOfMessagesToRead = 5;
		int numberOfMessagesReadSoFar = 0;
		boolean keepOnReading = true;

		// poll for new data
		while(keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for(ConsumerRecord record : records) {
				numberOfMessagesReadSoFar++;
				logger.info("Key:[{}], Value:[{}] Partition {} Offset {}", record.key(), record.value(),
						record.partition(), record.offset());

				if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
					keepOnReading = false;
					break;
				}
			}
		}
		logger.info("Exiting the Application");
	}
}

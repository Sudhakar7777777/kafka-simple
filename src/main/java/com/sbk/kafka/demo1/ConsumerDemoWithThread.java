package com.sbk.kafka.demo1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

	private ConsumerDemoWithThread() {
	}

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
	}

	private void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-app-3";
		String topicName = "first_topic";

		// latch for dealing with multiple threads
		CountDownLatch latch = new CountDownLatch(1);

		logger.info("Creating the consumer thread");
		// create the consumer runnable
		Runnable myConsumerRunnable = new ConsumerRunnable(
				bootstrapServers,
				groupId,
				topicName,
				latch
		);

		// start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			}
			catch(InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}
		));

		try {
			latch.await();
		} catch(InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	public static class ConsumerRunnable implements Runnable {
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
		private CountDownLatch latch;
		KafkaConsumer<String, String> consumer;

		ConsumerRunnable(String bootstrapServers, String groupId, String topicName, CountDownLatch latch) {
			// create consumer configs
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			// create consumer
			consumer = new KafkaConsumer<>(properties);

			// subscribe consumer to out topic(s)
			consumer.subscribe(Collections.singleton(topicName));

			//initializer
			this.latch = latch;

			logger.info("Runnable constructed.");
		}

		@Override
		public void run() {
			logger.info("Runnable begin run()");
			try {
				// poll for new data
				while(true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for(ConsumerRecord record : records) {
						logger.info("Key:[{}], Value:[{}] Partition {} Offset {}", record.key(), record.value(),
								record.partition(), record.offset());
					}
				}
			} catch(WakeupException e) {
				logger.info("Received shutdown signal!");
			} finally {
				consumer.close();
				// tell our main code we are down with consumer
				latch.countDown();
			}
		}

		public void shutdown() {
			// the wakeup() method is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			consumer.wakeup();
		}
	}
}

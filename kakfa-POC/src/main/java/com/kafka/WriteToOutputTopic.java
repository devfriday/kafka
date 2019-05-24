package com.kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.kafka.constants.IKafkaConstants;
import com.kafka.consumer.ConsumerCreator;
import com.kafka.producer.ProducerCreator;

public class WriteToOutputTopic {
	public static void main(String[] args) {
		//runProducer();
		runConsumer1();
	}

	static void runConsumer1() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				//System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				//System.out.println("Record partition " + record.partition());
				//System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
		}
		consumer.close();
	}



}

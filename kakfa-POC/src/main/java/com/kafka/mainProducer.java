package com.kafka;

import com.kafka.constants.IKafkaConstants;
import com.kafka.producer.ProducerCreator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

/**
 * This will produce to Kafka Input topic will behave like input
 */

public class mainProducer {

    public static void main(String[] args) {
        runProducer();

    }

    static void runProducer() {
        Producer<Long, String> producer = ProducerCreator.createProducer();

        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.INPUT_TOPIC,
                    "This is record " + index);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());

            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
            finally {
                try {
                    Thread.sleep(500);
                }
                catch (Exception e){
                    System.out.println("prob here");
                }
            }
        }
    }
}

package com.kafka.verticles;

import com.kafka.constants.IKafkaConstants;
import com.kafka.producer.ProducerCreator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

/**
 * This verticle will write to OutputTopic
 **/
public class ProducerVerticle extends AbstractVerticle {




    @Override
    public void start(Future<Void> startFuture) throws Exception {
        vertx.eventBus().consumer("Record2", message -> {
            System.out.println(" Msg Received by PRODUCER : " + message.body());

            Producer<Long, String> producer = ProducerCreator.createProducer();

                final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.OUTPUT_TOPIC, message.body().toString() );
                try {
                    RecordMetadata metadata = producer.send(record).get();
                    System.out.println("Record Getting Writteb to Output Topic " + metadata.toString());

                } catch (ExecutionException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                } catch (InterruptedException e) {
                    System.out.println("Error in sending record");
                    System.out.println(e);
                }


        });
        System.out.println("ProcessorVerticle started");
    }

    @Override
    public void stop() throws Exception {
        System.out.println("ProducerVerticle stopped");
    }

}
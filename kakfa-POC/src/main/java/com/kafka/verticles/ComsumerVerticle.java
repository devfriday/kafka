package com.kafka.verticles;


import com.kafka.constants.IKafkaConstants;
import com.kafka.consumer.ConsumerCreator;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * This verticle will read from Input Tpoic and start event loop.
 **/

public class ComsumerVerticle extends AbstractVerticle {

   // Vertx vertx;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
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
                // System.out.println("Record Key " + record.key());
                System.out.println("Record value Read From Input " + record.value());

                //vertx.eventBus().publish("Record", record.value());
                vertx.eventBus().send("Record", record.value());
                //System.out.println("Record partition " + record.partition());
                //System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();

        System.out.println("ComsumerVerticle started");
        runConsumer();
    }

    @Override
    public void stop() throws Exception {
        System.out.println("ComsumerVerticle stopped");
    }

   static void runConsumer() {

    }

}
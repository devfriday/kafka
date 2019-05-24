package com.kafka.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;

/**
 * This verticle will make changes in the object do some processing
 **/
public class ProcessorVerticle extends AbstractVerticle {



    @Override
    public void start(Future<Void> startFuture) throws Exception {
        vertx.eventBus().consumer("Record", message -> {
            System.out.println(" Msg Received by Processor : " + message.body());
            vertx.eventBus().send("Record2", message.body().toString().toUpperCase());
        });
        System.out.println("ProcessorVerticle started");
    }

    @Override
    public void stop() throws Exception {
        System.out.println("ProcessorVerticle stopped");
    }

}
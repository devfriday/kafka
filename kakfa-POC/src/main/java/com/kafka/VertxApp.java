package com.kafka;

import com.kafka.producer.ProducerCreator;
import com.kafka.verticles.ComsumerVerticle;
import com.kafka.verticles.ProcessorVerticle;
import com.kafka.verticles.ProducerVerticle;
import io.vertx.core.Vertx;

public class VertxApp {

  public static void main(String[] args) throws InterruptedException {

    Vertx vertx = Vertx.vertx();

    vertx.deployVerticle(new ComsumerVerticle(), stringAsyncResult -> {
      System.out.println("ComsumerVerticle  deployment complete");
    });

    vertx.deployVerticle(new ProcessorVerticle(), stringAsyncResult -> {
      System.out.println("ProcessorVerticle  deployment complete");
    });

    //Thread.sleep(2000);

    vertx.deployVerticle(new ProducerVerticle(), stringAsyncResult -> {
      System.out.println("ProducerVerticle deployment complete");
    });

  }

}

package org.example;

import org.apache.kafka.common.errors.TimeoutException;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {

    /**
     * Send and receive 1000 msgs
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        new KaAdmin().createTopic(KafkaProperties.TOPIC);

        int recordCount = 1000;
        CountDownLatch latch = new CountDownLatch(2);

        //start consumer
        KaConsumer consumer = new KaConsumer(latch);
        new Thread(() -> {
            consumer.consume(recordCount);
        }).start();

        //start producer
        new Thread(() -> {
            Random random = new Random();
            new KaProducer(latch).send(
                    Arrays.stream(random.ints(recordCount, 0, 1000000000).toArray()).boxed()
                            .map(i -> "msg " + String.valueOf(i)).collect(Collectors.toList()), true);
        }).start();

        //wait for completion
        if (!latch.await(5, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }

        consumer.shutdown();

        System.out.println("finished");
    }
}
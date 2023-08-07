package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KaConsumer implements ConsumerRebalanceListener {

    private final KafkaConsumer<Integer, String> consumer;
    private final CountDownLatch latch;

    private volatile boolean shutdown=false;

    public KaConsumer(CountDownLatch latch){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KaConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        this.latch = latch;
    }

    public void consume(final int countToCheck){
        consumer.subscribe(Collections.singletonList(KafkaProperties.TOPIC), this);
        int index = 0;
        while(!shutdown){
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                ++index;
                System.out.println(" received message : "+index+" from partition " + record.partition() + ", (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
            if(index >= countToCheck){
                shutdown();
            }
        }
    }

    public void shutdown() {
        shutdown = true;
        latch.countDown();
        //this.consumer.close();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Revoking partitions:" + partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Assigning partitions:" + partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}

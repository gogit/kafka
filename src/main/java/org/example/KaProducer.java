package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KaProducer {


    private final KafkaProducer<Integer, String> producer;
    private final CountDownLatch latch;
    public KaProducer(CountDownLatch latch){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
           props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        producer = new KafkaProducer<>(props);
        this.latch = latch;
    }

    public void send(List<String> messages, boolean sync){
        if(sync){
            int num = 0;
            for(String msg: messages) {
                Future<RecordMetadata> future = send(++num, msg);
                try {
                    future.get();
                    System.out.println("Sent message: (" + num + ", " + msg + ")");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }else{
            int num = 0;
            for(String msg: messages) {
                sendAsync(++num, msg, System.currentTimeMillis());
            }
        }
        //finished producing
        latch.countDown();
    }

    private Future<RecordMetadata> send(final int messageKey, final String messageStr) {
        return producer.send(new ProducerRecord<>(KafkaProperties.TOPIC,
                messageKey,
                messageStr));
    }
    private void sendAsync(final int messageKey, final String messageStr, final long currentTimeMs) {
        this.producer.send(new ProducerRecord<>(KafkaProperties.TOPIC,
                        messageKey,
                        messageStr),
                new CompletionCallBack(currentTimeMs, messageKey, messageStr));
    }

    class CompletionCallBack implements Callback {

        private final long startTime;
        private final int key;
        private final String message;

        public CompletionCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method will
         * be called when the record sent to the server has been acknowledged. When exception is not null in the callback,
         * metadata will contain the special -1 value for all fields except for topicPartition, which will be valid.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). An empty metadata
         *                  with -1 value for all fields except for topicPartition will be returned if an error occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println(
                        "message(" + key + ", " + message + ") sent to partition(" + metadata.partition() +
                                "), " +
                                "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }
}

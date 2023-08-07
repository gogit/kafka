package org.example;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KaAdmin {
    Admin adminClient;

    public KaAdmin(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);

        adminClient = Admin.create(props);

    }

    public void createTopic(String name){
        final short replicationFactor = 1;
        int numPartitions = 2;

        CreateTopicsResult topicsResult = adminClient.createTopics(Arrays.asList(new NewTopic(KafkaProperties.TOPIC, numPartitions, replicationFactor)));

        System.out.println(topicsResult.values());
    }

    public void deleteTopics(final List<String> topicsToDelete)
            throws InterruptedException, ExecutionException {
        try {
            adminClient.deleteTopics(topicsToDelete).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
            System.out.println("Encountered exception during topic deletion: " + e.getCause());
        }
        System.out.println("Deleted old topics: " + topicsToDelete);
    }
}

package org.acme;

//import io.smallrye.mutiny.vertx.kafka.client.KafkaConsumer;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.kafka.client.consumer.KafkaConsumer;

import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.mutiny.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerClient {

    static volatile String last;

    public static void main(String[] args) {
        // Create a Vert.x instance
        Vertx vertx = Vertx.vertx();

        // key.deserializer=org.apache.kafka.common.serialization.StringDeserializer, bootstrap.servers=10.11.88.2:9092, value.deserializer=org.apache.kafka.common.serialization.StringDeserializer, auto.offset.reset=earliest, group.id=my-group}
        //2023-05-03 11:51:11,463 WARN

        // Define the Kafka consumer configuration
        String bootstrapServers = "10.11.88.2:9092";
        String topic = "hello-vertex";
        String groupId = "my-mutiny-client";

        Map<String, String> map = new HashMap<>();;
        map.put("key.deserializer", StringDeserializer.class.getName());
        map.put("value.deserializer", StringDeserializer.class.getName());
        map.put("bootstrap.servers", bootstrapServers);
        map.put("group.id", groupId);
        map.put("enable.auto.commit", "false");
        map.put("auto.offset.reset", "earliest");


        int partition = 0;

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, map, String.class, String.class);

        // Assign a topic partition to the consumer
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(tp);


        consumer.subscribe(Collections.singleton(topic))
                .onItem().transformToMulti(x -> consumer.toMulti())
                .subscribe().with(
                        record -> {
                            System.out.printf("Polled Record:(%s, %s, %d, %d)\n",
                                    record.key(), record.value(),
                                    record.partition(), record.offset());
                            last = record.key() + "-" + record.value();

                            consumer.commitAndForget();
                        }
                );

        /*
        // Subscribe to the topic
        consumer.subscribe(topic).await().indefinitely();

        // Start consuming messages
        consumer.toMulti()
                .onItem()
                .transformToMulti(record -> {
                    System.out.printf("Received message: %s%n", record.value());
                    return Multi.createFrom().item(consumer.committed(tp).onItem().ignore());
                    //return consumer.committed(tp).onItem().ignore();
                    //return consumer.commit().onItem().ignore();
                    //©;
                })
                .merge()
                .subscribe().with(
                        throwable -> System.err.printf("Error: %s%n", throwable)
                );*/
    }
}

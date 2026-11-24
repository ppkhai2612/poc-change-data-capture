package com.example;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.util.Properties;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class FeatureUsageApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "feature-usage-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Configure the app to create topics that don't exist
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        props.put(StreamsConfig.topicPrefix(StreamsConfig.CONSUMER_PREFIX) + "auto.offset.reset", "latest");

        // Add configuration for internal topics
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);  // Set to 1 for single broker
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0); // No standby replicas
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // Commit more frequently
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10 * 1024 * 1024); // 10MB cache

        // Make sure required topics exist
        createTopicsIfNotExist(props);

        StreamsBuilder builder = new StreamsBuilder();

        // Configure Serdes fr input topic
        KStream<String, String> stream = builder.stream(
            "saas.public.feature_usage",
            Consumed.with(Serdes.String(), Serdes.String())
        );

        // Process the stream to count feature usage
        KTable<String, Long> counts = stream
            .mapValues(value -> {
                try {
                    JsonObject jsonObject = JsonParser.parseString(value).getAsJsonObject();
                    // Handle Debezium format - data is in the "after" field
                    if (jsonObject.has("after")) {
                        JsonObject after = jsonObject.getAsJsonObject("after");
                        if (after.has("feature_name")) {
                            return after.get("feature_name").getAsString();
                        }
                    }
                    // Fallback if JSON structure doesn't match expected format
                    System.err.println("Unexpected JSON structure: " + value);
                    return "unknown";
                } catch (Exception e) {
                    System.err.println("Error parsing JSON: " + e.getMessage() + ", value: " + value);
                    e.printStackTrace();
                    return "unknown";
                }
            })
            .filter((key, featureName) -> !featureName.equals("unknown"))
            .groupBy((key, feature) -> feature, Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("feature-usage-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        // Explicitly configure Serdes for output topic
        counts.toStream().mapValues(count -> 
            "{\"count\": " + count + "}"
        ).to(
            "feature_usage_counts", 
            Produced.with(Serdes.String(), Serdes.String())
        );

        // Print topology for debugging
        Topology topology = builder.build();
        System.out.println("Topology: " + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        
        // Add shutdown hook to close streams gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        
        // Set UncaughtExceptionHandler with correct interface
        streams.setUncaughtExceptionHandler(throwable -> {
            System.err.println("Uncaught exception: " + throwable.getMessage());
            throwable.printStackTrace();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        
        // Set StateListener
        streams.setStateListener((newState, oldState) -> {
            System.out.println("State changed from " + oldState + " to " + newState);
        });
        
        // Clean up local state before starting
        streams.cleanUp();
        
        streams.start();
        System.out.println("Kafka Streams application started");
    }

    private static void createTopicsIfNotExist(Properties props) {
        String bootstrapServers = props.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // Check if source topic exists, create it if it doesn't
            String sourceTopic = "saas.public.feature_usage";
            String targetTopic = "feature_usage_counts";
            String repartitionTopic = "feature-usage-stream-feature-usage-store-repartition";
            String storeTopic = "feature-usage-stream-feature-usage-store-changelog";

            try {
                System.out.println("Checking if topics exist: " + sourceTopic + ", " + targetTopic);

                // Get list of existing topics
                Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);

                List<NewTopic> topicsToCreate = new ArrayList<>();

                if (!existingTopics.contains(sourceTopic)) {
                    System.out.println("Creating topic: " + sourceTopic);
                    topicsToCreate.add(new NewTopic(sourceTopic, 1, (short) 1));
                }
                
                if (!existingTopics.contains(targetTopic)) {
                    System.out.println("Creating topic: " + targetTopic);
                    topicsToCreate.add(new NewTopic(targetTopic, 1, (short) 1));
                }

                // Also create the internal topics that Kafka Streams needs
                if (!existingTopics.contains(repartitionTopic)) {
                    System.out.println("Creating internal topic: " + repartitionTopic);
                    topicsToCreate.add(new NewTopic(repartitionTopic, 1, (short) 1));
                }
                
                if (!existingTopics.contains(storeTopic)) {
                    System.out.println("Creating internal topic: " + storeTopic);
                    topicsToCreate.add(new NewTopic(storeTopic, 1, (short) 1));
                }

                if (!topicsToCreate.isEmpty()) {
                    CreateTopicsOptions options = new CreateTopicsOptions().validateOnly(false);
                    CreateTopicsResult result = adminClient.createTopics(topicsToCreate, options);
                    result.all().get(30, TimeUnit.SECONDS);
                    System.out.println("Topics created successfully");
                } else {
                    System.out.println("All required topics already exist");
                }
            } catch (Exception e) {
                System.out.println("Error createing topics: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
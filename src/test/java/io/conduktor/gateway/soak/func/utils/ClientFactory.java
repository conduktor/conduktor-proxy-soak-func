package io.conduktor.gateway.soak.func.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ClientFactory implements Closeable {

    public Properties gatewayProperties;
    private final List<AutoCloseable> closeables;
    public Properties kafkaProperties;
    public String tenant;

    public static ClientFactory generateClientFactory(String clientId, Map<String, String> properties) {

        return new ClientFactory(getGatewayProperties(clientId, properties), getKafkaProperties());
    }

    private static Properties getGatewayProperties(String clientId, Map<String, String> properties) {
        Properties clientProperties = new Properties();
        clientProperties.put("bootstrap.servers", "localhost:6969");
        clientProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        clientProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        clientProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        clientProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        clientProperties.put("client.id", clientId);
        if (Objects.nonNull(properties)) {
            clientProperties.putAll(properties);
        }
        return clientProperties;
    }

    private static Properties getKafkaProperties() {
        Properties clientProperties = new Properties();
        clientProperties.put("bootstrap.servers", "localhost:29092");
        return clientProperties;
    }

    public ClientFactory(Properties gatewayProperties, Properties kafkaProperties) {
        this.closeables = new ArrayList<>();
        this.gatewayProperties = gatewayProperties;
        this.kafkaProperties = kafkaProperties;
    }

    public ClientFactory(Properties gatewayProperties, Properties kafkaProperties, String tenant) {
        this.closeables = new ArrayList<>();
        this.gatewayProperties = gatewayProperties;
        this.kafkaProperties = kafkaProperties;
        this.tenant = tenant;
    }

    public AdminClient gatewayAdmin() {
        var adminClient = AdminClient.create(gatewayProperties);
        closeables.add(adminClient);
        return adminClient;
    }

    public AdminClient kafkaAdmin() {
        var adminClient = AdminClient.create(kafkaProperties);
        closeables.add(adminClient);
        return adminClient;
    }

    public KafkaProducer<String, String> gatewayProducer() {
        return producer(gatewayProperties);
    }

    public void addGatewayPropertyOverride(String name, String value) {
        gatewayProperties.put(name, value);
    }

    public KafkaProducer<String, String> kafkaProducer() {
        return producer(kafkaProperties);
    }

    public KafkaConsumer<String, String> gatewayConsumer(final String groupId) {
        return consumer(gatewayProperties, groupId);
    }

    public KafkaConsumer<String, String> gatewayConsumer(final String groupId, final String groupInstanceId) {
        return consumer(gatewayProperties, groupId, groupInstanceId);
    }

    public KafkaConsumer<String, String> kafkaConsumer(final String groupId) {
        return consumer(kafkaProperties, groupId);
    }


    @Override
    public void close() {
        var iterator = closeables.iterator();
        while (iterator.hasNext()) {
            try {
                var closeable = iterator.next();
                closeable.close();
                iterator.remove();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private KafkaProducer<String, String> producer(Properties producerProperties) {
        var producer = new KafkaProducer<String, String>(producerProperties, new StringSerializer(), new StringSerializer());
        closeables.add(producer);
        return producer;
    }

    private KafkaConsumer<String, String> consumer(Properties consumerProperties, final String groupId) {
        final Map<String, Object> configs = new HashMap<String, Object>() {{
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }};
        return consumer(consumerProperties, configs);
    }

    private KafkaConsumer<String, String> consumer(Properties consumerProperties, Map<String, Object> consumerConfigs) {
        Properties clientProperties = new Properties();
        clientProperties.putAll(consumerProperties);
        clientProperties.putAll(consumerConfigs);
        dumpConfig("Consumer", consumerConfigs);
        var consumer = new KafkaConsumer<>(clientProperties, new StringDeserializer(), new StringDeserializer());
        closeables.add(consumer);
        return consumer;
    }


    private KafkaConsumer<String, String> consumer(Properties consumerProperties, final String groupId, final String groupInstanceId) {
        final Map<String, Object> configs = new HashMap<String, Object>() {{
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }};
        return consumer(consumerProperties, configs);
    }


    private void dumpConfig(final String type, final Map<String, Object> config) {
        log.info("{}:\n{}", type, config.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("\n")));
    }


}

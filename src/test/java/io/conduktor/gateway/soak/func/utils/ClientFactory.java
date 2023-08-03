package io.conduktor.gateway.soak.func.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ClientFactory implements Closeable {

    private final List<AutoCloseable> closeables;

    public ClientFactory() {
        this.closeables = new ArrayList<>();
    }

    private static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";

    public AdminClient gatewayAdmin(Map<String, String> properties) {
        var gatewayProperties = getDefaultGatewayProperties(properties);
        if (Objects.nonNull(properties)) {
            gatewayProperties.putAll(properties);
        }
        var admin = AdminClient.create(gatewayProperties);
        closeables.add(admin);
        return admin;
    }

    @NotNull
    private static Properties getDefaultGatewayProperties(Map<String, String> properties) {
        var gatewayProperties = new Properties();
        gatewayProperties.put("bootstrap.servers", "localhost:6969");
        gatewayProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        gatewayProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        gatewayProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        gatewayProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        gatewayProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        gatewayProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        gatewayProperties.put("client.id", "clientId");
        if (Objects.nonNull(properties)) {
            gatewayProperties.putAll(properties);
            if (properties.containsKey(SECURITY_PROTOCOL_CONFIG)) {
                if (properties.get(SECURITY_PROTOCOL_CONFIG).equals(SecurityProtocol.SASL_SSL.name())) {
                    properties.put("ssl.truststore.location", "config/tls/sasl-ssl/truststore.jks");
                    properties.put("ssl.truststore.password", "changeit");
                } else if (properties.get(SECURITY_PROTOCOL_CONFIG).equals(SecurityProtocol.SSL.name())) {
                    properties.put("ssl.truststore.location", "config/tls/ssl/truststore.jks");
                    properties.put("ssl.truststore.password", "changeit");
                }
            }
        }
        return gatewayProperties;
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

    @NotNull
    private static Properties getDefaultKafkaProperties() {
        var kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:29092");
        return kafkaProperties;
    }

    public AdminClient kafkaAdmin(Map<String, String> properties) {
        var kafkaProperties = getDefaultKafkaProperties();
        if (Objects.nonNull(properties)) {
            kafkaProperties.putAll(properties);
        }
        var admin = AdminClient.create(kafkaProperties);
        closeables.add(admin);
        return AdminClient.create(kafkaProperties);
    }

    public KafkaProducer<String, String> gatewayProducer(Map<String, String> properties) {
        var gatewayProperties = getDefaultGatewayProperties(properties);
        if (Objects.nonNull(properties)) {
            gatewayProperties.putAll(properties);
        }
        return producer(gatewayProperties);
    }

    public KafkaProducer<String, String> kafkaProducer(Map<String, String> properties) {
        var kafkaProperties = getDefaultKafkaProperties();
        if (Objects.nonNull(properties)) {
            kafkaProperties.putAll(properties);
        }
        return producer(kafkaProperties);
    }

    public KafkaConsumer<String, String> gatewayConsumer(final String groupId, Map<String, String> properties) {
        var gatewayProperties = getDefaultGatewayProperties(properties);
        if (Objects.nonNull(properties)) {
            gatewayProperties.putAll(properties);
        }
        return consumer(gatewayProperties, groupId);
    }

    public KafkaConsumer<String, String> kafkaConsumer(final String groupId, Map<String, String> properties) {
        var kafkaProperties = getDefaultKafkaProperties();
        if (Objects.nonNull(properties)) {
            kafkaProperties.putAll(properties);
        }
        return consumer(kafkaProperties, groupId);
    }

    private KafkaProducer<String, String> producer(Properties producerProperties) {
        var producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new StringSerializer());
        closeables.add(producer);
        return producer;
    }

    private KafkaConsumer<String, String> consumer(Properties consumerProperties, final String groupId) {
        final Map<String, Object> configs = new HashMap<>() {{
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


    private static void dumpConfig(final String type, final Map<String, Object> config) {
        log.info("{}:\n{}", type, config.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("\n")));
    }


}

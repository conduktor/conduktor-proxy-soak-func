package io.conduktor.gateway.soak.func.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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

@Slf4j
public class ClientFactory implements Closeable {

    private final List<AutoCloseable> closeables;

    public ClientFactory() {
        this.closeables = new ArrayList<>();
    }

    private static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";

    public AdminClient gatewayAdmin(Properties properties) {
        var admin = AdminClient.create(properties);
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
        gatewayProperties.put("auto.offset.reset", "earliest");
        gatewayProperties.put("enable.auto.commit", "false");
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

    public AdminClient kafkaAdmin(Properties properties) {
        var admin = AdminClient.create(properties);
        closeables.add(admin);
        return AdminClient.create(properties);
    }

    public KafkaProducer<String, String> kafkaProducer(Properties properties) {
        var producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
        closeables.add(producer);
        return producer;
    }

    public  KafkaConsumer<String, String> consumer(Properties properties) {
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer());
        closeables.add(consumer);
        return consumer;
    }
}

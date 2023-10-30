package io.conduktor.gateway.soak.func.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ClientFactory implements Closeable {

    private final List<AutoCloseable> closeables = new ArrayList<>();

    private final Map<String, AdminClient> adminCache = new ConcurrentHashMap<>();
    private final Map<String, KafkaProducer<String, String>> producerCache = new ConcurrentHashMap<>();
    private final Map<String, KafkaConsumer<String, String>> consumerCache = new ConcurrentHashMap<>();

    public AdminClient kafkaAdmin(Properties properties) {
        return adminCache.computeIfAbsent(properties.toString(), k -> track(AdminClient.create(properties)));
    }

    public KafkaProducer<String, String> kafkaProducer(Properties properties) {
        return producerCache.computeIfAbsent(properties.toString(), k -> track(new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())));
    }

    public KafkaConsumer<String, String> consumer(Properties properties) {
        return consumerCache.computeIfAbsent(properties.toString(), k -> track(new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())));
    }

    private <T extends AutoCloseable> T track(T t) {
        closeables.add(t);
        return t;
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
}

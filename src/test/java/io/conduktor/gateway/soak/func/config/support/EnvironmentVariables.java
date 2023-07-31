package io.conduktor.gateway.soak.func.config.support;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.System.*;

@Slf4j
public class EnvironmentVariables {

    private static final Pattern ENV_VAR_PATTERN = Pattern.compile("(?i)\\$\\{([a-z0-9_]+)(\\s*\\|(.+)\\s*)?\\}");
    public static Properties resolve(Properties properties) {
        var propertiesResult = new Properties();
        properties.forEach((key, value) -> propertiesResult.put(key, resolve((String) value)));
        return propertiesResult;
    }

    public static String resolve(String input) {
        var resolved = input;
        resolved = resolve(resolved, ENV_VAR_PATTERN.matcher(input), EnvironmentVariables::resolveEnvVar);
        return resolved;
    }

    private static String resolve(String input, Matcher matcher, Function<String, String> resolver) {

        var sb = new StringBuilder();

        var lastIndex = 0;
        while (matcher.find()) {
            var start = matcher.start();
            var end = matcher.end();

            var envName = matcher.group(1);
            var envValue = resolver.apply(envName);
            if (StringUtils.isEmpty(envValue)) {
                if (Objects.nonNull(matcher.group(2))) {
                    envValue = matcher.group(3);
                } else {
                    log.warn("Environment variable `{}` cannot be resolved", envName);
                }
            }

            if (start > lastIndex)
                sb.append(input, lastIndex, start);

            sb.append(envValue);

            lastIndex = end;
        }

        if (lastIndex < input.length())
            sb.append(input.substring(lastIndex));

        return sb.toString().trim();
    }

    private static String resolveEnvVar(@NonNull String envName) {
        return Optional.ofNullable(getenv(envName))
                .or(() -> Optional.ofNullable(getProperty(envName)))
                .orElse(null);
    }

    public static void setSystemProperties(File file) {
        try (var input = new FileInputStream(file)) {
            setSystemProperties(input);
        } catch (Exception e) {
            throw new IllegalArgumentException("Error while loading file", e);
        }
    }

    public static void setSystemProperties(InputStream input) {
        var props = new Properties();
        try {
            props.load(input);
        } catch (IOException e) {
            throw new IllegalArgumentException("Error while loading input stream", e);
        }
        setSystemProperties(props);
    }

    public static void setSystemProperties(Properties props) {
        props.entrySet().stream() //
                .filter(entry -> entry.getValue() != null) //
                .forEach(entry -> setProperty(entry.getKey().toString(), entry.getValue().toString()));
    }

    private static Set<String> getValidPropertyKeys() {
        var validPropertyKeys = new HashSet<String>();
        validPropertyKeys.addAll(ConsumerConfig.configNames());
        validPropertyKeys.addAll(ProducerConfig.configNames());
        validPropertyKeys.addAll(AdminClientConfig.configNames());
        return validPropertyKeys;
    }

}

package io.conduktor.gateway.soak.func.utils;

import io.conduktor.gateway.soak.func.ScenarioTest;
import io.conduktor.gateway.soak.func.config.Scenario;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@Getter
public class DockerComposeUtils {

    private static final String DOCKER_COMPOSE_FILE_PATH = "/docker-compose.yaml";
    private static final String DOCKER_COMPOSE_FOLDER_PATH = "/docker-compose/";
    public static final String GATEWAY_SECURITY_PROTOCOL = "GATEWAY_SECURITY_PROTOCOL";

    public static void stopContainer() {
        //composeContainer.stop();
    }

    @NotNull
    public static String getUpdatedDockerCompose(Scenario.Service kafkaConfigs, Scenario.Service gatewayConfigs) throws IOException {
        var yaml = new Yaml();
        var composeConfig = yaml.load(ScenarioTest.class.getResourceAsStream(DOCKER_COMPOSE_FILE_PATH));

        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "kafka1", kafkaConfigs);
        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "kafka2", kafkaConfigs);
        appendEnvironments((LinkedHashMap<String, Object>) composeConfig, "gateway", gatewayConfigs);

        return yaml.dump(composeConfig);
    }

    private static void appendEnvironments(LinkedHashMap<String, Object> composeConfig, String name, Scenario.Service service) {
        // Update the environment variables for the specified service
        var services = (Map<String, Object>) composeConfig.get("services");
        if (services.containsKey(name)) {
            var serviceConfig = (Map<String, Object>) services.get(name);
            serviceConfig.put("image", service.getImage());
            var environment = (Map<String, String>) serviceConfig.computeIfAbsent("environment", k -> new LinkedHashMap<>());
            environment.putAll(service.getEnvironment());
        } else {
            throw new IllegalArgumentException("Service '" + name + "' not found in the docker-compose.yml file.");
        }
    }
}

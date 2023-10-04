package io.conduktor.gateway.soak.func.utils;

import io.conduktor.gateway.soak.func.ScenarioTest;
import io.conduktor.gateway.soak.func.config.Scenario;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.util.ArrayList;
import java.util.HashMap;
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
    public static String getUpdatedDockerCompose(Scenario scenario) {
        DumperOptions dumperOptions = new DumperOptions();
        dumperOptions.setPrettyFlow(true);
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        dumperOptions.setIndent(2);

        var yaml = new Yaml(dumperOptions);

        var composeConfig = yaml.load(ScenarioTest.class.getResourceAsStream(DOCKER_COMPOSE_FILE_PATH));
        scenario.getServices().forEach((name, service) -> appendEnvironments((LinkedHashMap<String, Object>) composeConfig, name, service));
        return yaml.dump(composeConfig);
    }

    private static void appendEnvironments(LinkedHashMap<String, Object> composeConfig, String name, Scenario.Service service) {
        var services = (Map<String, Map<String, Object>>) composeConfig.get("services");
        Map<String, Object> serviceMap = new HashMap<>();
        if (services.containsKey(name)) {
            serviceMap = services.get(name);
        }
        ((Map<String, Map<String, Object>>) composeConfig.get("services")).put(name, serviceMap);

        Map<String, Object> docker = service.getDocker();
        if (docker != null) {
            for (String key : docker.keySet()) {
                Object value = docker.get(key);
                if (value instanceof String) {
                    serviceMap.put(key, value);
                } else if (value instanceof LinkedHashMap) {
                    LinkedHashMap o = (LinkedHashMap) serviceMap.get(key);
                    if (o == null) {
                        o = new LinkedHashMap();
                    }
                    o.putAll((LinkedHashMap) value);
                    serviceMap.put(key, o);
                } else if (value instanceof ArrayList) {
                    ArrayList o = (ArrayList) serviceMap.get(key);
                    if (o == null) {
                        o = new ArrayList();
                    }
                    o.addAll((ArrayList) value);
                    serviceMap.put(key, o);
                } else {
                    throw new RuntimeException(key + ", we don't support " + value.getClass() + " yet");
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> mergeRecursively(Map<String, Object> map1, Map<String, Object> map2) {
        Map<String, Object> mergedMap = new HashMap<>(map1);
        for (String key : map2.keySet()) {
            if (map1.containsKey(key) && map1.get(key) instanceof Map && map2.get(key) instanceof Map) {
                mergedMap.put(key, mergeRecursively((Map<String, Object>) map1.get(key), (Map<String, Object>) map2.get(key)));
            } else {
                mergedMap.put(key, map2.get(key));
            }
        }
        return mergedMap;
    }
}

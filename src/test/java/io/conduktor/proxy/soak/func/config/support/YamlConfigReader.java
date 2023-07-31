package io.conduktor.proxy.soak.func.config.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class YamlConfigReader<ConfigType> {

    private final Class<ConfigType> type;

    public static <T> YamlConfigReader<T> forType(Class<T> type) {
        return new YamlConfigReader<>(type);
    }

    public ConfigType readYaml(String yamlFilePath) throws IOException {
        var path = Paths.get(yamlFilePath);
        var lines = Files.lines(path);
        var data = lines.collect(Collectors.joining("\n"));
        var resolvedData = EnvironmentVariables.resolve(data);
        lines.close();
        var mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        return mapper.readValue(resolvedData, type);
    }

    public ConfigType readYamlInResources(String yamlFilePath) throws IOException {
        // The class loader that loaded the class
        var all = stringFromFile(yamlFilePath);
        var resolvedData = EnvironmentVariables.resolve(all);
        var mapper = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID));
        mapper.findAndRegisterModules();
        return mapper.readValue(resolvedData, type);
    }

    private String stringFromFile(String yamlFilePath) {
        try {
            var configPath = Paths.get(yamlFilePath);
            var configFile = configPath.toFile();
            if (configFile.exists() && configFile.isFile()) {
                return Files.readString(configPath);
            } else {
                var resourceConfigStream = getClass().getClassLoader().getResourceAsStream(yamlFilePath);
                if (resourceConfigStream == null) {
                    throw new FileNotFoundException(yamlFilePath + " is not a valid configuration file");
                }
                return IOUtils.toString(resourceConfigStream, StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("error when reading file " + yamlFilePath, e);
        }
    }


}

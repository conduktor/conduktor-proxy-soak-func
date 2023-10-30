package io.conduktor.gateway.soak.func.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PluginResponse {
    private String pluginClass;
    private int priority;
    private String name;
    private Map<String, Object> config;
}

package io.conduktor.gateway.soak.func.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PluginRequest {
    private String pluginClass;
    private int priority;
    private Map<String, Object> config;
}

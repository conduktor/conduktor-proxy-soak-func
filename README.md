# Conduktor Gateway Soak Functionalities

Testing your Conduktor Gateway by your own scenario (yaml file in config/scenario directory)

## Scenario YAML File Description

The [scenario3.yaml](config/scenario/scenario3.yaml) file in this config/scenario folder repository is a configuration
file used to define a test workflow for testing functionalities related to SASL_SSL and Dynamic Header Injection in
Conduktor Gateway. The YAML file contains various sections that define the setup and actions to be performed during the
test.<br/>
E.g:

```yaml
title: Simple test with SASL_SSL and Dynamic Header Injection
docker:
  kafka:
    version: latest
    environment:
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: false
  gateway:
    version: 2.0.0-amd64
    environment:
      GATEWAY_SECURITY_PROTOCOL: SASL_SSL
plugins:
  franklx:
    inject-header-1:
      pluginClass: "io.conduktor.gateway.interceptor.DynamicHeaderInjectionPlugin"
      "priority": 1
      "config":
        "topic": ".*"
        "headers":
          "X-RAW_KEY": "a value"
    inject-header-2:
      pluginClass: "io.conduktor.gateway.interceptor.DynamicHeaderInjectionPlugin"
      "priority": 100
      "config":
        "topic": ".*"
        "headers":
          "X-USERNAME": "{{user}}"
actions:
  - type: CREATE_TOPIC
    target: GATEWAY
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"franklx\" password=\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImZyYW5rbHgiLCJ2Y2x1c3RlciI6ImZyYW5rbHgiLCJleHAiOjExNjkyMDg4MjI0fQ.RaDEBfXwUJHKYiDeOGQ8HgLT7K9yCnNa6SckSvHZuCw\";"
    topic: topic1
  - type: PRODUCE
    target: GATEWAY
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"franklx\" password=\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImZyYW5rbHgiLCJ2Y2x1c3RlciI6ImZyYW5rbHgiLCJleHAiOjExNjkyMDg4MjI0fQ.RaDEBfXwUJHKYiDeOGQ8HgLT7K9yCnNa6SckSvHZuCw\";"
    topic: topic1
    messages:
      - key: "key3"
        value: "value3"
        headers:
          header_key1: "headerValue5"
          header_key2: "headerValue5"
  - type: FETCH
    target: KAFKA
    topic: franklxtopic1
    assertions:
      - key:
          expected: "key3"
        value:
          expected: "value3"
        headers:
          header_key1:
            expected: "headerValue5"
          header_key2:
            expected: "headerValue5"
          X-RAW_KEY:
            expected: "a value"
          X-USERNAME:
            expected: "franklx"
  - type: FETCH
    target: GATEWAY
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"franklx\" password=\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImZyYW5rbHgiLCJ2Y2x1c3RlciI6ImZyYW5rbHgiLCJleHAiOjExNjkyMDg4MjI0fQ.RaDEBfXwUJHKYiDeOGQ8HgLT7K9yCnNa6SckSvHZuCw\";"
    topic: topic1
    assertions:
      - key:
          expected: "key3"
        value:
          expected: "value3"
        headers:
          header_key1:
            expected: "headerValue5"
          header_key2:
            expected: "headerValue5"
          X-RAW_KEY:
            expected: "a value"
          X-USERNAME:
            expected: "franklx"
```

### Title

The YAML file starts with a title that gives an overview of the purpose of the test

```yaml
title: Simple test with SASL_SSL and Dynamic Header Injection
```

### Docker Configuration

The next section defines `docker` - the Docker configurations for Kafka and the Gateway Service. It specifies the
versions and environment variables to be used for these containers:

```yaml
docker:
  kafka:
    version: latest
    environment:
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: false
  gateway:
    version: 2.0.0-amd64
    environment:
      GATEWAY_SECURITY_PROTOCOL: SASL_SSL
```

### Plugins

In this `plugins` section, the YAML file defines the plugins for tenant `franklx` to be used during the test.
Specifically, it sets up two instances of the `DynamicHeaderInjectionPlugin`, each with its own configuration:

```yaml
plugins:
  franklx:
    inject-header-1:
      pluginClass: "io.conduktor.gateway.interceptor.DynamicHeaderInjectionPlugin"
      "priority": 1
      "config":
        "topic": ".*"
        "headers":
          "X-RAW_KEY": "a value"
    inject-header-2:
      pluginClass: "io.conduktor.gateway.interceptor.DynamicHeaderInjectionPlugin"
      "priority": 100
      "config":
        "topic": ".*"
        "headers":
          "X-USERNAME": "{{user}}"
```

### Actions

The actions section contains a list of actions to be performed during the test. <br/>
Each action specifies its:

- `type` (currently support: `CREATE_TOPIC`, `PRODUCE`, `FETCH`)
- `target` (`KAFKA`, `GATEWAY`)
- `properties` (admin client configs, producer configs or consumer configs depends on your action type)
- `topic` (topic name)
- `messages` (list of messages, each messages contains `key`, `value` and `headers`)
- `assertions` (list of assertions)
    - `description` (string): A brief description of the assertion being made. This helps provide context and
      understanding.
    - `headers`(List<String, Assertion>): A map of headers and their associated assertions.
    - key (Assertion): An instance of the `Assertion` class used to assert conditions on key or identifiers within the
      record.
    - value (Assertion): Another instance of the `Assertion` class used to assert conditions on value within the record.

The `Assertion` defines the criteria for asserting conditions. It contains the following properties:

- `operator` (String): This specifies the type of assertion operation to be performed. Possible values include:
    - `isEqualTo`
    - `isNotEqualTo`
    - `satisfies`: allows you to specify complex conditions that must be met for the assertion to pass. The `expected`
      property contains an expression written in a custom syntax that supports logical and comparison operations on data
      attributes. For example: `'[name] == "conduktor" && [password] != "password1"'`
    - `isNull`
    - `isNotNull`
    - and more
- `expected` (Object): This property holds the expected value against which the assertion will be evaluated. It can be
  of any data type relevant to the assertion.

These actions include:

#### Action 1: Create topic `topic1` for tenant `franklx` in the Gateway Service:

```yaml
  - type: CREATE_TOPIC
    target: GATEWAY
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"franklx\" password=\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImZyYW5rbHgiLCJ2Y2x1c3RlciI6ImZyYW5rbHgiLCJleHAiOjExNjkyMDg4MjI0fQ.RaDEBfXwUJHKYiDeOGQ8HgLT7K9yCnNa6SckSvHZuCw\";"
    topic: topic1
```

#### Action 2: Producing a message to `topic1` for tenant `franklx` in the Gateway Service:

```yaml
  - type: PRODUCE
    target: GATEWAY
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"franklx\" password=\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImZyYW5rbHgiLCJ2Y2x1c3RlciI6ImZyYW5rbHgiLCJleHAiOjExNjkyMDg4MjI0fQ.RaDEBfXwUJHKYiDeOGQ8HgLT7K9yCnNa6SckSvHZuCw\";"
    topic: topic1
    messages:
      - key: "key3"
        value: "value3"
        headers:
          header_key1: "headerValue5"
          header_key2: "headerValue5"

```

#### Action 3: Fetching messages from `franklxtopic1` in the Kafka and expect return exactly defined messages:

```yaml
  - type: FETCH
    target: KAFKA
    topic: franklxtopic1
    assertions:
      - key:
          expected: "key3"
        value:
          expected: "value3"
        headers:
          header_key1:
            expected: "headerValue5"
          header_key2:
            expected: "headerValue5"
          X-RAW_KEY:
            expected: "a value"
          X-USERNAME:
            expected: "franklx"
```

#### Action 4: Fetching messages from `topic1` in the Gateway Service and expect return exactly defined messages:

```yaml
  - type: FETCH
    target: GATEWAY
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"franklx\" password=\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VybmFtZSI6ImZyYW5rbHgiLCJ2Y2x1c3RlciI6ImZyYW5rbHgiLCJleHAiOjExNjkyMDg4MjI0fQ.RaDEBfXwUJHKYiDeOGQ8HgLT7K9yCnNa6SckSvHZuCw\";"
    topic: topic1
    assertions:
      - key:
          expected: "key3"
        value:
          expected: "value3"
        headers:
          header_key1:
            expected: "headerValue5"
          header_key2:
            expected: "headerValue5"
          X-RAW_KEY:
            expected: "a value"
          X-USERNAME:
            expected: "franklx"
```

# Feel free to explore and extend this repository for more test scenarios and functionalities as per your requirements!

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.kafka;

import io.airlift.log.Logger;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.NodeManager;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.inject.Inject;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Kafka nodes. A worker may connect to multiple Kafka nodes depending on the segments and partitions
 * it needs to process. According to the Kafka source code, a KafkaConsumer {@link KafkaConsumer} is not thread-safe.
 */
public class KafkaConsumerManager
{
    private static final Logger log = Logger.get(KafkaConsumerManager.class);
    private static final String byteArrayDeserializer = ByteArrayDeserializer.class.getName();
    private static final String kafkaAvroDeserializer = KafkaAvroDeserializer.class.getName();

    private final String connectorId;
    private final NodeManager nodeManager;
    private final int sessionTimeoutMs;
    private final int maxPartitionFetchBytes;
    private final String securityProtocol;
    private String trustStoreLocation = "";
    private String trustStorePassword = "";
    private String keyStoreLocation = "";
    private String keyStorePassword = "";
    private String keyPassword = "";
    private final boolean autoCommit;
    private final String bootStrapServers;
    private String schemaRegistryUrl = "";

    @Inject
    public KafkaConsumerManager(
            String connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.sessionTimeoutMs = kafkaConnectorConfig.getSessionTimeoutMs();
        this.maxPartitionFetchBytes = kafkaConnectorConfig.getMaxPartitionFetchBytes();
        this.securityProtocol = kafkaConnectorConfig.getSecurityProtocol();
        this.trustStoreLocation = kafkaConnectorConfig.getTrustStoreLocation();
        this.trustStorePassword = kafkaConnectorConfig.getTrustStorePassword();
        this.keyStoreLocation = kafkaConnectorConfig.getKeyStoreLocation();
        this.keyStorePassword = kafkaConnectorConfig.getKeyStorePassword();
        this.keyPassword = kafkaConnectorConfig.getKeyPassword();
        this.autoCommit = kafkaConnectorConfig.isAutoCommit();
        this.bootStrapServers = bootstrapServers(kafkaConnectorConfig.getBootstrapServers());
        this.schemaRegistryUrl = kafkaConnectorConfig.getSchemaRegistryUrl();
    }

    public <V> KafkaConsumer<byte[], V> getConsumer(Boolean isAvro)
    {
        return getConsumer(isAvro, bootStrapServers);
    }

    public <V> KafkaConsumer<byte[], V> getConsumer(Boolean isAvro, Set<HostAddress> nodes)
    {
        return getConsumer(isAvro, bootstrapServers(nodes));
    }

    private <V> KafkaConsumer<byte[], V> getConsumer(Boolean isAvro, String newBootstrapServers)
    {
        try {
            Thread.currentThread().setContextClassLoader(null);
            Properties prop = new Properties();
            prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, newBootstrapServers);
            prop.put(ConsumerConfig.GROUP_ID_CONFIG, connectorId + "-presto");
            prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
            prop.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
            prop.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
            prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, byteArrayDeserializer);
            if (isAvro) {
                prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaAvroDeserializer);
            }
            else {
                prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, byteArrayDeserializer);
            }
            prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            prop.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer_" + connectorId + "_" + nodeManager.getCurrentNode().getNodeIdentifier()
                    + "_" + ThreadLocalRandom.current().nextInt() + "_" + System.currentTimeMillis());
            prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            prop.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation);
            prop.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
            prop.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation);
            prop.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
            prop.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPassword);
            prop.put("schema.registry.url", schemaRegistryUrl);
            return new KafkaConsumer<>(prop);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String bootstrapServers(Set<HostAddress> nodes)
    {
        return nodes.stream().map(ha -> new StringJoiner(":").add(ha.getHostText()).add(String.valueOf(ha.getPort())).toString())
                .collect(Collectors.joining(","));
    }
}

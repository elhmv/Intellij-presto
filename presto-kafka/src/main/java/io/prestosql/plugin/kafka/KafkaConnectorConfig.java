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

//import com.facebook.presto.spi.HostAddress;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.io.File;
import java.util.Set;

import static com.google.common.collect.Iterables.transform;

public class KafkaConnectorConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    /**
     * Seed bootstrapServers for Kafka cluster. At least one must exist.
     */
    private Set<HostAddress> bootstrapServers = ImmutableSet.of();

    /**
     * The timeout used to detect consumer failures when using Kafka's group management facility.
     */
    private int sessionTimeoutMs = 10000;

    /**
     * The maximum amount of data per-partition the server will return.
     */
    private int maxPartitionFetchBytes = 1048576;

    /**
     * The schema name to use in the connector.
     */
    private String defaultSchema = "default";

    /**
     * Set of tables known to this connector. For each table, a description file may be present in the catalog folder which describes columns for the given topic.
     */
    private Set<String> tableNames = ImmutableSet.of();

    /**
     * Folder holding the JSON description files for Kafka topics.
     */
    private File tableDescriptionDir = new File("etc/kafka/");

    /**
     * Whether internal columns are shown in table metadata or not. Default is no.
     */
    private boolean hideInternalColumns = true;

    /**
     * Number of messages per split
     */
    private int messagesPerSplit = 100_000;

    /**
     * Security protocol to connect to the broker, default is plain text
     */
    private String securityProtocol = "PLAINTEXT";

    private String trustStoreLocation = "";
    private String trustStorePassword = "";
    private String keyStoreLocation = "";
    private String keyStorePassword = "";
    private String keyPassword = "";

    private Duration kafkaConnectTimeout = Duration.valueOf("10s");
    private DataSize kafkaBufferSize = DataSize.of(64, DataSize.Unit.KILOBYTE);

    private boolean autoCommit = true;

    private String schemaRegistryUrl = "";

    @NotNull
    public File getTableDescriptionDir()
    {
        return tableDescriptionDir;
    }

    @Config("kafka.table-description-dir")
    public KafkaConnectorConfig setTableDescriptionDir(File tableDescriptionDir)
    {
        this.tableDescriptionDir = tableDescriptionDir;
        return this;
    }

    @NotNull
    public Set<String> getTableNames()
    {
        return tableNames;
    }

    @Config("kafka.table-names")
    public KafkaConnectorConfig setTableNames(String tableNames)
    {
        this.tableNames = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(tableNames));
        return this;
    }

    @NotNull
    public String getDefaultSchema()
    {
        return defaultSchema;
    }

    @Config("kafka.default-schema")
    public KafkaConnectorConfig setDefaultSchema(String defaultSchema)
    {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getBootstrapServers()
    {
        return bootstrapServers;
    }

    @Config("kafka.bootstrap-servers")
    public KafkaConnectorConfig setBootstrapServers(String bootstrapServers)
    {
        this.bootstrapServers = (bootstrapServers == null) ? null : parseNodes(bootstrapServers);
        return this;
    }

    @Min(1000)
    public int getSessionTimeoutMs()
    {
        return sessionTimeoutMs;
    }

    @Config("kafka.session-timeout-ms")
    public KafkaConnectorConfig setSessionTimeoutMs(String sessionTimeoutMs)
    {
        this.sessionTimeoutMs = Integer.valueOf(sessionTimeoutMs);
        return this;
    }

    public int getMaxPartitionFetchBytes()
    {
        return maxPartitionFetchBytes;
    }

    @Config("kafka.max-partition-fetch-bytes")
    public KafkaConnectorConfig setMaxPartitionFetchBytes(String maxPartitionFetchBytes)
    {
        this.maxPartitionFetchBytes = Integer.valueOf(maxPartitionFetchBytes);
        return this;
    }

    public boolean isHideInternalColumns()
    {
        return hideInternalColumns;
    }

    @Config("kafka.hide-internal-columns")
    public KafkaConnectorConfig setHideInternalColumns(boolean hideInternalColumns)
    {
        this.hideInternalColumns = hideInternalColumns;
        return this;
    }

    public static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return ImmutableSet.copyOf(transform(splitter.split(nodes), KafkaConnectorConfig::toHostAddress));
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }

    @Config("kafka.security-protocol")
    public KafkaConnectorConfig setSecurityProtocol(String securityProtocol)
    {
        this.securityProtocol = securityProtocol;
        return this;
    }

    public String getSecurityProtocol()
    {
        return securityProtocol;
    }

    @Config("kafka.ssl-truststore-location")
    public KafkaConnectorConfig setTrustStoreLocation(String trustStoreLocation)
    {
        this.trustStoreLocation = trustStoreLocation;
        return this;
    }

    public String getTrustStoreLocation()
    {
        return trustStoreLocation;
    }

    @Config("kafka.ssl-truststore-password")
    public KafkaConnectorConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    @Config("kafka.ssl-keystore-location")
    public KafkaConnectorConfig setKeyStoreLocation(String keyStoreLocation)
    {
        this.keyStoreLocation = keyStoreLocation;
        return this;
    }

    public String getKeyStoreLocation()
    {
        return keyStoreLocation;
    }

    @Config("kafka.ssl-keystore-password")
    public KafkaConnectorConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("kafka.ssl-key-password")
    public KafkaConnectorConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    @MinDuration("1s")
    public Duration getKafkaConnectTimeout()
    {
        return kafkaConnectTimeout;
    }

    @Config("kafka.connect-timeout")
    @ConfigDescription("Kafka connection timeout")
    public KafkaConnectorConfig setKafkaConnectTimeout(String kafkaConnectTimeout)
    {
        this.kafkaConnectTimeout = Duration.valueOf(kafkaConnectTimeout);
        return this;
    }

    public DataSize getKafkaBufferSize()
    {
        return kafkaBufferSize;
    }

    @Config("kafka.buffer-size")
    @ConfigDescription("Kafka message consumer buffer size")
    public KafkaConnectorConfig setKafkaBufferSize(String kafkaBufferSize)
    {
        this.kafkaBufferSize = DataSize.valueOf(kafkaBufferSize);
        return this;
    }

    @Config("kafka.auto-commit")
    public KafkaConnectorConfig setAutoCommit(boolean autoCommit)
    {
        this.autoCommit = autoCommit;
        return this;
    }

    public boolean isAutoCommit()
    {
        return autoCommit;
    }

    @Config("kafka.schema-registry-url")
    public KafkaConnectorConfig setSchemaRegistryUrl(String schemaRegistryUrl)
    {
        this.schemaRegistryUrl = schemaRegistryUrl;
        return this;
    }

    public String getSchemaRegistryUrl()
    {
        return schemaRegistryUrl;
    }

    public int getMessagesPerSplit()
    {
        return messagesPerSplit;
    }

    @Config("kafka.messages-per-split")
    @ConfigDescription("Count of Kafka messages to be processed by single Presto Kafka connector split")
    public KafkaConnectorConfig setMessagesPerSplit(int messagesPerSplit)
    {
        this.messagesPerSplit = messagesPerSplit;
        return this;
    }
}

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
package com.facebook.presto.kafka;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

public class TestKafkaConnectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(KafkaConnectorConfig.class)
                .setBootstrapServers("")
                .setSessionTimeoutMs("10000")
                .setMaxPartitionFetchBytes("1048576")
                .setDefaultSchema("default")
                .setTableNames("")
                .setTableDescriptionDir(new File("etc/kafka/"))
                .setHideInternalColumns(true)
                .setAutoCommit(true)
                .setSecurityProtocol("PLAINTEXT")
                .setTrustStoreLocation("")
                .setTrustStorePassword("")
                .setKeyStoreLocation("")
                .setKeyStorePassword("")
                .setKeyPassword("")
                .setSchemaRegistryUrl(""));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.table-description-dir", "/var/lib/kafka")
                .put("kafka.table-names", "table1, table2, table3")
                .put("kafka.default-schema", "kafka")
                .put("kafka.bootstrap-servers", "localhost:12345,localhost:23456")
                .put("kafka.session-timeout-ms", "100000")
                .put("kafka.max-partition-fetch-bytes", "104857600")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.auto-commit", "false")
                .put("kafka.security-protocol", "SASL_PLAINTEXT")
                .put("kafka.ssl-truststore-location", "ssl-truststore-location")
                .put("kafka.ssl-truststore-password", "ssl-truststore-password")
                .put("kafka.ssl-keystore-location", "ssl-keystore-location")
                .put("kafka.ssl-keystore-password", "ssl-keystore-password")
                .put("kafka.ssl-key-password", "ssl-key-password")
                .put("kafka.schema-registry-url", "schema-registry-url")
                .build();

        KafkaConnectorConfig expected = new KafkaConnectorConfig()
                .setTableDescriptionDir(new File("/var/lib/kafka"))
                .setTableNames("table1, table2, table3")
                .setDefaultSchema("kafka")
                .setBootstrapServers("localhost:12345, localhost:23456")
                .setSessionTimeoutMs("100000")
                .setMaxPartitionFetchBytes("104857600")
                .setHideInternalColumns(false)
                .setAutoCommit(false)
                .setSecurityProtocol("SASL_PLAINTEXT")
                .setTrustStoreLocation("ssl-truststore-location")
                .setTrustStorePassword("ssl-truststore-password")
                .setKeyStoreLocation("ssl-keystore-location")
                .setKeyStorePassword("ssl-keystore-password")
                .setKeyPassword("ssl-key-password")
                .setSchemaRegistryUrl("schema-registry-url");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}

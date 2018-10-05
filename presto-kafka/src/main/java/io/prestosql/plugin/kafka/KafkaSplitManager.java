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

import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Kafka specific implementation of {@link ConnectorSplitManager}.
 */
public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(KafkaSplitManager.class);

    private final String connectorId;
    private final KafkaConsumerManager consumerManager;
    private final int messagesPerSplit;


    @Inject
    public <KafkaConnectorId> KafkaSplitManager(
            String connectorId,
            KafkaConnectorConfig kafkaConnectorConfig,
            KafkaConsumerManager consumerManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");
        messagesPerSplit = requireNonNull(kafkaConnectorConfig, "kafkaConnectorConfig is null").getMessagesPerSplit();

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        //KafkaTableHandle kafkaTableHandle = convertLayout(layout).getTable();
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerManager.getConsumer(false)) {
            List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(kafkaTableHandle.getTopicName());
            List<TopicPartition> topicPartitions = partitionInfoList.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());
            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);
            Optional<String> keyDataSchemaContents = kafkaTableHandle.getKeyDataSchemaLocation()
                    .map(KafkaSplitManager::readSchema);
            Optional<String> messageDataSchemaContents = kafkaTableHandle.getMessageDataSchemaLocation()
                    .map(KafkaSplitManager::readSchema);

            for (PartitionInfo partitionInfo : partitionInfoList) {
                log.debug("Adding Partition %s/%s", partitionInfo.topic(), partitionInfo.partition());
                if (partitionInfo.leader() == null) { // Leader election going on...
                    log.warn("No leader for partition %s/%s found!", partitionInfo.topic(), partitionInfo.partition());
                    continue;
                }
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
//                List<TopicPartition> topicPartitions = ImmutableList.of(topicPartition);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());
                new Range(partitionBeginOffsets.get(topicPartition), partitionEndOffsets.get(topicPartition))
                        .partition(messagesPerSplit).stream()
                        .map(range -> new KafkaSplit(
                                kafkaTableHandle.getTopicName(),
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getMessageDataFormat(),
                                keyDataSchemaContents,
                                messageDataSchemaContents,
                                partitionInfo.partition(),
                                range,
                                leader))
                        .forEach(splits::add);
                }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }

    private static String readSchema(String dataSchemaLocation)
    {
        InputStream inputStream = null;
        try {
            if (isURI(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
                try {
                    inputStream = new URL(dataSchemaLocation).openStream();
                }
                catch (MalformedURLException e) {
                    // try again before failing
                    inputStream = new FileInputStream(dataSchemaLocation);
                }
            }
            else {
                inputStream = new FileInputStream(dataSchemaLocation);
            }
            return CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Could not parse the Avro schema at: " + dataSchemaLocation, e);
        }
        finally {
            closeQuietly(inputStream);
        }
    }

    private static void closeQuietly(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static boolean isURI(String location)
    {
        try {
            //noinspection ResultOfMethodCallIgnored
            URI.create(location);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }
}

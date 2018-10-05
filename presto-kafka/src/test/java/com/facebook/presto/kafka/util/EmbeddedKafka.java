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
package com.facebook.presto.kafka.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.facebook.presto.kafka.util.TestUtils.findUnusedPort;
import static com.facebook.presto.kafka.util.TestUtils.toProperties;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class EmbeddedKafka
        implements Closeable
{
    private final EmbeddedZookeeper zookeeper;
    private final int port;
    private final File kafkaDataDir;
    private final KafkaServerStartable kafka;

    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();

    public static EmbeddedKafka createEmbeddedKafka()
            throws IOException
    {
        return new EmbeddedKafka(new EmbeddedZookeeper(), new Properties());
    }

    public static EmbeddedKafka createEmbeddedKafka(Properties overrideProperties)
            throws IOException
    {
        return new EmbeddedKafka(new EmbeddedZookeeper(), overrideProperties);
    }

    EmbeddedKafka(EmbeddedZookeeper zookeeper, Properties overrideProperties)
            throws IOException
    {
        this.zookeeper = requireNonNull(zookeeper, "zookeeper is null");
        requireNonNull(overrideProperties, "overrideProperties is null");

        this.port = findUnusedPort();
        this.kafkaDataDir = Files.createTempDir();

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("broker.id", "0")
                .put("host.name", "localhost")
                .put("offsets.topic.replication.factor", "1")
                .put("offsets.topic.num.partitions", "1")
                .put("num.partitions", "2")
                .put("log.flush.interval.messages", "10000")
                .put("log.flush.interval.ms", "1000")
                .put("log.retention.minutes", "60")
                .put("log.segment.bytes", "1048576")
                .put("auto.create.topics.enable", "true")
                .put("zookeeper.connection.timeout.ms", "1000000")
                .put("port", Integer.toString(port))
                .put("log.dirs", kafkaDataDir.getAbsolutePath())
                .put("zookeeper.connect", zookeeper.getConnectString())
                .putAll(Maps.fromProperties(overrideProperties))
                .build();

        KafkaConfig config = new KafkaConfig(toProperties(properties));
        this.kafka = new KafkaServerStartable(config);
    }

    public void start()
            throws InterruptedException, IOException
    {
        if (!started.getAndSet(true)) {
            zookeeper.start();
            kafka.startup();
        }
    }

    @Override
    public void close()
    {
        if (started.get() && !stopped.getAndSet(true)) {
            kafka.shutdown();
            kafka.awaitShutdown();

            try {
                zookeeper.close();

                java.nio.file.Files.walk(kafkaDataDir.toPath())
                        .map(Path::toFile)
                        .sorted(Comparator.comparing(File::isDirectory))
                        .forEach(File::delete);
            }
            catch (IOException ignored) {
            }
        }
    }

    public void createTopics(String... topics)
    {
        createTopics(2, 1, new CreateTopicsOptions(), topics);
    }

    public void createTopics(int partitions, int replication, CreateTopicsOptions createTopicsOptions, String... topics)
    {
        checkState(started.get() && !stopped.get(), "not started!");

        try (AdminClient adminClient = createAdminClient()) {
            adminClient.createTopics(Arrays.stream(topics).map(t -> new NewTopic(t, partitions, (short) replication)).collect(Collectors.toList()), createTopicsOptions);
        }
    }

    public CloseableProducer<Long, Object> createProducer()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bootstrap.servers", getConnectString())
                .put("key.serializer", NumberEncoder.class.getName())
                .put("value.serializer", JsonEncoder.class.getName())
                .put("partitioner.class", NumberPartitioner.class.getName())
                .put("acks", "1")
                .build();

        Properties producerConfig = toProperties(properties);
        return new CloseableProducer<>(producerConfig);
    }

    public AdminClient createAdminClient()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bootstrap.servers", getConnectString())
                .build();

        Properties adminClientConfig = toProperties(properties);
        return KafkaAdminClient.create(adminClientConfig);
    }

    public static class CloseableProducer<K, V>
            extends KafkaProducer<K, V>
            implements AutoCloseable
    {
        public CloseableProducer(Properties config)
        {
            super(config);
        }
    }

    public String getConnectString()
    {
        return "localhost:" + Integer.toString(port);
    }
}

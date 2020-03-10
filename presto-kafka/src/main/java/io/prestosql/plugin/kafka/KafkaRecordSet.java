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

//import com.facebook.presto.decoder.DecoderColumnHandle;
//import com.facebook.presto.decoder.FieldValueProvider;
//import com.facebook.presto.decoder.RowDecoder;
//import com.facebook.presto.spi.ColumnHandle;
//import com.facebook.presto.spi.RecordCursor;
//import com.facebook.presto.spi.RecordSet;
//import com.facebook.presto.spi.block.Block;
//import com.facebook.presto.spi.type.Type;

//import com.facebook.presto.kafka.KafkaConsumerManager;
//import com.facebook.presto.kafka.KafkaInternalFieldDescription;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.NoWrappingJsonEncoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.FieldValueProviders.booleanValueProvider;
import static io.prestosql.decoder.FieldValueProviders.bytesValueProvider;
import static io.prestosql.decoder.FieldValueProviders.longValueProvider;
import static java.util.Objects.requireNonNull;

//import static com.facebook.presto.decoder.FieldValueProviders.booleanValueProvider;
//import static com.facebook.presto.decoder.FieldValueProviders.bytesValueProvider;
//import static com.facebook.presto.decoder.FieldValueProviders.longValueProvider;

/**
 * Kafka specific record set. Returns a cursor for a topic which iterates over a Kafka partition segment.
 */
public class KafkaRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(KafkaRecordSet.class);

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final long CONSUMER_POLL_TIMEOUT = 100L;

    private final KafkaSplit split;
    private final KafkaConsumerManager consumerManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;

    private final List<KafkaColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    KafkaRecordSet(KafkaSplit split,
                   KafkaConsumerManager consumerManager,
                   List<KafkaColumnHandle> columnHandles,
                   RowDecoder keyDecoder,
                   RowDecoder messageDecoder)
    {
        this.split = requireNonNull(split, "split is null");

        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor();
    }

    public class KafkaRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;
        //private long cursorOffset = split.getStart();
        private long cursorOffset = split.getMessagesRange().getBegin();
        private final AtomicBoolean reported = new AtomicBoolean();
        private Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator;

        private final FieldValueProvider[] currentRowValues = new FieldValueProvider[columnHandles.size()];

        KafkaRecordCursor()
        {
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (true) {
                if (cursorOffset >= split.getMessagesRange().getEnd()) {
                    return endOfData(); // Split end is exclusive.
                }
                // Create a fetch request
                if (split.getTopicName().endsWith(".avro") || split.getTopicName().contains(".avro.")) {
                    openAvroFetchRequest();
                }
                else {
                    openFetchRequest();
                }

                while (consumerRecordIterator.hasNext()) {
                    ConsumerRecord<byte[], byte[]> messageAndOffset = consumerRecordIterator.next();
                    long messageOffset = messageAndOffset.offset();
                    if (messageOffset >= split.getMessagesRange().getEnd()) {
                        return endOfData(); // Past our split end. Bail.
                    }

                    if (messageOffset >= cursorOffset) {
                        return nextRow(messageAndOffset);
                    }
                }
                consumerRecordIterator = null;
            }
        }

        private boolean endOfData()
        {
            if (!reported.getAndSet(true)) {
                log.debug("Found a total of %d messages with %d bytes (%d messages expected). Last Offset: %d (%d, %d)",
                        totalMessages, totalBytes, split.getMessagesRange().getEnd() - split.getMessagesRange().getBegin(), cursorOffset, split.getMessagesRange().getBegin(),
                        split.getMessagesRange().getEnd());
            }
            return false;
        }

        private boolean nextRow(ConsumerRecord<byte[], byte[]> consumerRecord)
        {
            cursorOffset = consumerRecord.offset() + 1;
            totalBytes += consumerRecord.serializedValueSize();
            totalMessages++;

            byte[] keyData = EMPTY_BYTE_ARRAY;
            byte[] messageData = EMPTY_BYTE_ARRAY;

            if (consumerRecord.key() != null) {
                ByteBuffer key = ByteBuffer.wrap(consumerRecord.key());
                keyData = new byte[key.remaining()];
                key.get(keyData);
            }

            if (consumerRecord.value() != null) {
                ByteBuffer message = ByteBuffer.wrap(consumerRecord.value());
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

            Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedKey = keyDecoder.decodeRow(keyData, null);
            Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue = messageDecoder.decodeRow(messageData, null);

            for (DecoderColumnHandle columnHandle : columnHandles) {
                if (columnHandle.isInternal()) {
                    KafkaInternalFieldDescription fieldDescription = KafkaInternalFieldDescription.forColumnName(columnHandle.getName());
                    switch (fieldDescription) {
                        case SEGMENT_COUNT_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(totalMessages));
                            break;
                        case PARTITION_OFFSET_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(consumerRecord.offset()));
                            break;
                        case MESSAGE_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(messageData));
                            break;
                        case MESSAGE_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(messageData.length));
                            break;
                        case KEY_FIELD:
                            currentRowValuesMap.put(columnHandle, bytesValueProvider(keyData));
                            break;
                        case KEY_LENGTH_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(keyData.length));
                            break;
                        case KEY_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedKey.isPresent()));
                            break;
                        case MESSAGE_CORRUPT_FIELD:
                            currentRowValuesMap.put(columnHandle, booleanValueProvider(!decodedValue.isPresent()));
                            break;
                        case PARTITION_ID_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getPartitionId()));
                            break;
                        case SEGMENT_START_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getMessagesRange().getBegin()));
                            break;
                        case SEGMENT_END_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(split.getMessagesRange().getEnd()));
                            break;
                        case MESSAGE_TIMESTAMP_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(consumerRecord.timestamp()));
                            break;
                        case MESSAGE_TIMESTAMP_TYPE_FIELD:
                            currentRowValuesMap.put(columnHandle, longValueProvider(consumerRecord.timestampType().id));
                            break;
                        default:
                            throw new IllegalArgumentException("unknown internal field " + fieldDescription);
                    }
                }
            }

            decodedKey.ifPresent(currentRowValuesMap::putAll);
            decodedValue.ifPresent(currentRowValuesMap::putAll);

            for (int i = 0; i < columnHandles.size(); i++) {
                ColumnHandle columnHandle = columnHandles.get(i);
                currentRowValues[i] = currentRowValuesMap.get(columnHandle);
            }

            return true; // Advanced successfully.
        }

        @Override
        public boolean getBoolean(int field)
        {
            return getFieldValueProvider(field, boolean.class).getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            return getFieldValueProvider(field, long.class).getLong();
        }

        @Override
        public double getDouble(int field)
        {
            return getFieldValueProvider(field, double.class).getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            return getFieldValueProvider(field, Slice.class).getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            return getFieldValueProvider(field, Block.class).getBlock();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return currentRowValues[field] == null || currentRowValues[field].isNull();
        }

        private FieldValueProvider getFieldValueProvider(int field, Class<?> expectedType)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            checkFieldType(field, expectedType);
            return currentRowValues[field];
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
        }

        private void openFetchRequest()
        {
            if (consumerRecordIterator != null) {
                return;
            }
            ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> consumerRecordsBuilder = ImmutableList.builder();
            try (KafkaConsumer<byte[], byte[]> consumer = consumerManager.getConsumer(false, ImmutableSet.of(split.getLeader()))) {
                TopicPartition topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
                consumer.assign(ImmutableList.of(topicPartition));
                long nextOffset = split.getMessagesRange().getBegin();
                consumer.seek(topicPartition, split.getMessagesRange().getBegin());
                while (nextOffset <= split.getMessagesRange().getEnd() - 1) {
                    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));
                    consumerRecordsBuilder.addAll(consumerRecords.records(topicPartition));
                    nextOffset = consumer.position(topicPartition);
                }
            }
            consumerRecordIterator = consumerRecordsBuilder.build().iterator();
        }

        private void openAvroFetchRequest()
        {
            if (consumerRecordIterator != null) {
                return;
            }
            ImmutableList.Builder<ConsumerRecord<byte[], byte[]>> consumerRecordsBuilder = ImmutableList.builder();

            try (KafkaConsumer<byte[], GenericRecord> consumer = consumerManager.getConsumer(true, ImmutableSet.of(split.getLeader()))) {
                TopicPartition topicPartition = new TopicPartition(split.getTopicName(), split.getPartitionId());
                consumer.assign(ImmutableList.of(topicPartition));
                long nextOffset = split.getMessagesRange().getBegin();
                consumer.seek(topicPartition, split.getMessagesRange().getBegin());
                while (nextOffset <= split.getMessagesRange().getEnd() - 1) {
                    ConsumerRecords<byte[], GenericRecord> consumerRecords = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));
                    consumerRecords.records(topicPartition).forEach(record -> {
                        ConsumerRecord<byte[], byte[]> newRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), record.timestamp(), record.timestampType(),
                                0L, record.serializedKeySize(), record.serializedValueSize(), record.key(), convertToJson(record.value()));
                        consumerRecordsBuilder.add(newRecord);
                    });
                    nextOffset = consumer.position(topicPartition);
                }
            }
            consumerRecordIterator = consumerRecordsBuilder.build().iterator();
        }

        private byte[] convertToJson(GenericRecord record)
        {
            try {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                NoWrappingJsonEncoder jsonEncoder = new NoWrappingJsonEncoder(record.getSchema(), outputStream);
                new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, jsonEncoder);
                jsonEncoder.flush();
                return outputStream.toByteArray();
            }
            catch (IOException e) {
                throw new RuntimeException("Failed to convert to JSON.", e);
            }
        }
    }
}

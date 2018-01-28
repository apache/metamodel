/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.kafka;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class KafkaDataContext<K, V> extends QueryPostprocessDataContext {

    public static final String SYSTEM_PROPERTY_CONSUMER_POLL_TIMEOUT = "metamodel.kafka.consumer.poll.timeout";

    public static final String COLUMN_PARTITION = "partition";
    public static final String COLUMN_OFFSET = "offset";
    public static final String COLUMN_TIMESTAMP = "timestamp";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";

    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final ConsumerFactory consumerFactory;
    private final Supplier<Collection<String>> topicSupplier;

    public KafkaDataContext(Class<K> keyClass, Class<V> valueClass, String bootstrapServers,
            Collection<String> topics) {
        this(keyClass, valueClass, new KafkaConsumerFactory(bootstrapServers), () -> topics);
    }

    public KafkaDataContext(Class<K> keyClass, Class<V> valueClass, ConsumerFactory consumerFactory,
            Supplier<Collection<String>> topicSupplier) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.consumerFactory = consumerFactory;
        this.topicSupplier = topicSupplier;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(getMainSchemaName());

        final Collection<String> topics = topicSupplier.get();

        for (String topic : topics) {
            final MutableTable table = new MutableTable(topic, schema);
            table.addColumn(new MutableColumn(COLUMN_PARTITION, ColumnType.INTEGER));
            table.addColumn(new MutableColumn(COLUMN_OFFSET, ColumnType.BIGINT));
            table.addColumn(new MutableColumn(COLUMN_TIMESTAMP, ColumnType.TIMESTAMP));
            table.addColumn(new MutableColumn(COLUMN_KEY, ColumnTypeImpl.convertColumnType(keyClass)));
            table.addColumn(new MutableColumn(COLUMN_VALUE, ColumnTypeImpl.convertColumnType(valueClass)));
            schema.addTable(table);
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return "kafka";
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final String topic = table.getName();
        final Consumer<K, V> consumer = consumerFactory.createConsumer(topic, keyClass, valueClass);
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<TopicPartition> partitions = partitionInfos.stream().map(partitionInfo -> {
            return new TopicPartition(topic, partitionInfo.partition());
        }).collect(Collectors.toList());

        consumer.seekToBeginning(partitions);
        consumer.assign(partitions);

        if (maxRows > 0) {
            return new MaxRowsDataSet(new KafkaDataSet<K, V>(consumer, columns), maxRows);
        }
        return new KafkaDataSet<K, V>(consumer, columns);
    }

}

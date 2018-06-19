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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.annotations.InterfaceStability;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.FirstRowDataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

@InterfaceStability.Unstable
public class KafkaDataContext<K, V> extends QueryPostprocessDataContext implements UpdateableDataContext {

    public static final String SYSTEM_PROPERTY_CONSUMER_POLL_TIMEOUT = "metamodel.kafka.consumer.poll.timeout";

    public static final String COLUMN_PARTITION = "partition";
    public static final String COLUMN_OFFSET = "offset";
    public static final String COLUMN_TIMESTAMP = "timestamp";
    public static final String COLUMN_KEY = "key";
    public static final String COLUMN_VALUE = "value";

    private static final Set<OperatorType> OPTIMIZED_PARTITION_OPERATORS = new HashSet<>(Arrays.asList(
            OperatorType.EQUALS_TO, OperatorType.IN));
    private static final Set<OperatorType> OPTIMIZED_OFFSET_OPERATORS = new HashSet<>(Arrays.asList(
            OperatorType.GREATER_THAN, OperatorType.GREATER_THAN_OR_EQUAL));

    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final ConsumerAndProducerFactory consumerAndProducerFactory;
    private final Supplier<Collection<String>> topicSupplier;

    public KafkaDataContext(Class<K> keyClass, Class<V> valueClass, String bootstrapServers,
            Collection<String> topics) {
        this(keyClass, valueClass, new KafkaConsumerAndProducerFactory(bootstrapServers), () -> topics);
    }

    public KafkaDataContext(Class<K> keyClass, Class<V> valueClass,
            ConsumerAndProducerFactory consumerAndProducerFactory, Supplier<Collection<String>> topicSupplier) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.consumerAndProducerFactory = consumerAndProducerFactory;
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
        final Consumer<K, V> consumer = consumerAndProducerFactory.createConsumer(topic, keyClass, valueClass);
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        final List<TopicPartition> partitions = partitionInfos.stream().map(partitionInfo -> {
            return new TopicPartition(topic, partitionInfo.partition());
        }).collect(Collectors.toList());

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        final List<SelectItem> selectItems = columns.stream().map(col -> new SelectItem(col)).collect(Collectors
                .toList());

        return materializeMainSchemaTableFromConsumer(consumer, selectItems, 0, maxRows);
    }

    protected DataSet materializeMainSchemaTableFromConsumer(Consumer<K, V> consumer, List<SelectItem> selectItems,
            int offset, int maxRows) {
        DataSet dataSet = new KafkaDataSet<K, V>(consumer, selectItems);
        if (offset > 0) {
            dataSet = new FirstRowDataSet(dataSet, offset);
        }
        if (maxRows > 0) {
            dataSet = new MaxRowsDataSet(dataSet, maxRows);
        }
        return dataSet;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<SelectItem> selectItems, List<FilterItem> whereItems,
            int firstRow, int maxRows) {
        // check if we can optimize the consumption when either "partition" or "offset"
        // are in the where items.
        if (!whereItems.isEmpty()) {
            final boolean optimizable = whereItems.stream().allMatch(this::isOptimizable);
            if (optimizable) {
                long offset = 0;
                List<Integer> partitions = null;

                for (FilterItem whereItem : whereItems) {
                    final OperatorType operator = whereItem.getOperator();
                    switch (whereItem.getSelectItem().getColumn().getName()) {
                    case COLUMN_OFFSET:
                        if (operator == OperatorType.GREATER_THAN) {
                            offset = toLong(whereItem.getOperand()) + 1;
                        } else if (operator == OperatorType.GREATER_THAN_OR_EQUAL) {
                            offset = toLong(whereItem.getOperand());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    case COLUMN_PARTITION:
                        if (operator == OperatorType.EQUALS_TO) {
                            partitions = Arrays.asList(toInt(whereItem.getOperand()));
                        } else if (operator == OperatorType.IN) {
                            partitions = toIntList(whereItem.getOperand());
                        } else {
                            throw new UnsupportedOperationException();
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException();
                    }
                }

                final String topic = table.getName();
                final Consumer<K, V> consumer = consumerAndProducerFactory.createConsumer(topic, keyClass, valueClass);

                // handle partition filtering
                final List<TopicPartition> assignedPartitions;
                if (partitions == null) {
                    final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
                    assignedPartitions = partitionInfos.stream().map(partitionInfo -> {
                        return new TopicPartition(topic, partitionInfo.partition());
                    }).collect(Collectors.toList());
                } else {
                    assignedPartitions = partitions.stream().map(partitionNumber -> {
                        return new TopicPartition(topic, partitionNumber);
                    }).collect(Collectors.toList());
                }

                // handle offset filtering
                consumer.assign(assignedPartitions);
                if (offset == 0) {
                    consumer.seekToBeginning(assignedPartitions);
                } else {
                    for (TopicPartition topicPartition : assignedPartitions) {
                        consumer.seek(topicPartition, offset);
                    }
                }

                return materializeMainSchemaTableFromConsumer(consumer, selectItems, firstRow, maxRows);
            }
        }
        return super.materializeMainSchemaTable(table, selectItems, whereItems, firstRow, maxRows);
    }

    private static List<Integer> toIntList(Object operand) {
        if (operand == null) {
            return null;
        }
        if (operand.getClass().isArray()) {
            operand = Arrays.asList((Object[]) operand);
        }
        final List<Integer> list = new ArrayList<>();
        if (operand instanceof Iterable) {
            ((Iterable<?>) operand).forEach(o -> {
                list.add(toInt(o));
            });
        }
        return list;
    }

    private static int toInt(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        return Integer.parseInt(obj.toString());
    }

    private static long toLong(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        return Long.parseLong(obj.toString());
    }

    private boolean isOptimizable(FilterItem whereItem) {
        if (whereItem.isCompoundFilter()) {
            return false;
        }
        if (whereItem.getExpression() != null) {
            return false;
        }
        final SelectItem selectItem = whereItem.getSelectItem();
        if (selectItem.getExpression() != null || selectItem.getAggregateFunction() != null || selectItem
                .getScalarFunction() != null) {
            return false;
        }
        final Column column = selectItem.getColumn();
        if (column == null) {
            return false;
        }

        switch (column.getName()) {
        case COLUMN_OFFSET:
            return OPTIMIZED_OFFSET_OPERATORS.contains(whereItem.getOperator());
        case COLUMN_PARTITION:
            return OPTIMIZED_PARTITION_OPERATORS.contains(whereItem.getOperator());
        default:
            return false;
        }
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        final Producer<K, V> producer = consumerAndProducerFactory.createProducer(keyClass, valueClass);
        final KafkaUpdateCallback<K, V> callback = new KafkaUpdateCallback<>(this, producer);
        try {
            update.run(callback);
        } finally {
            callback.flush();
        }
        final UpdateSummary updateSummary = callback.getUpdateSummary();
        callback.close();
        return updateSummary;
    }
}

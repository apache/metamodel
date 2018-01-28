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

import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;

final class KafkaDataSet<K, V> extends AbstractDataSet {

    private final Consumer<K, V> consumer;
    private final long pollTimeout;

    private Iterator<ConsumerRecord<K, V>> currentIterator;
    private ConsumerRecord<K, V> currentRow;

    public KafkaDataSet(Consumer<K, V> consumer, List<SelectItem> selectItems) {
        super(new CachingDataSetHeader(selectItems));
        this.consumer = consumer;
        this.pollTimeout = Long.parseLong(System.getProperty(KafkaDataContext.SYSTEM_PROPERTY_CONSUMER_POLL_TIMEOUT,
                "1000"));
    }
    
    public Consumer<K, V> getConsumer() {
        return consumer;
    }

    @Override
    public boolean next() {
        if (currentIterator == null || !currentIterator.hasNext()) {
            final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
            if (records == null || records.isEmpty()) {
                return false;
            }
            currentIterator = records.iterator();
        }

        this.currentRow = currentIterator.next();
        if (currentRow == null) {
            return false;
        }
        return true;
    }

    @Override
    public Row getRow() {
        final Object[] values = getHeader().getSelectItems().stream().map(this::getValue).toArray();

        return new DefaultRow(getHeader(), values);
    }

    private Object getValue(SelectItem selectItem) {
        if (currentRow == null) {
            return null;
        }
        switch (selectItem.getColumn().getName()) {
        case KafkaDataContext.COLUMN_PARTITION:
            return currentRow.partition();
        case KafkaDataContext.COLUMN_OFFSET:
            return currentRow.offset();
        case KafkaDataContext.COLUMN_TIMESTAMP:
            return currentRow.timestamp();
        case KafkaDataContext.COLUMN_KEY:
            return currentRow.key();
        case KafkaDataContext.COLUMN_VALUE:
            return currentRow.value();
        }
        return null;
    }

    @Override
    public void close() {
        super.close();
        currentRow = null;
        consumer.unsubscribe();
        consumer.close();
    }
}

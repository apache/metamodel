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

import org.apache.kafka.clients.producer.Producer;
import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

final class KafkaUpdateCallback<K, V> extends AbstractUpdateCallback {

    private final Producer<K, V> producer;

    public KafkaUpdateCallback(KafkaDataContext<K, V> kafkaDataContext, Producer<K, V> producer) {
        super(kafkaDataContext);
        this.producer = producer;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDropTableSupported() {
        return false;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new KafkaInsertBuilder<>(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return false;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }
    
    public Producer<K, V> getProducer() {
        return producer;
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }

}

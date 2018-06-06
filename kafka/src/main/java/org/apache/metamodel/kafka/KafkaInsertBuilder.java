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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

final class KafkaInsertBuilder<K, V> extends AbstractRowInsertionBuilder<KafkaUpdateCallback<K, V>> {

    public KafkaInsertBuilder(KafkaUpdateCallback<K, V> updateCallback, Table table) {
        super(updateCallback, table);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() throws MetaModelException {
        final Column[] columns = getColumns();
        final Object[] values = getValues();

        K key = null;
        V value = null;

        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getName() == KafkaDataContext.COLUMN_KEY) {
                key = (K) values[i];
            }
            if (columns[i].getName() == KafkaDataContext.COLUMN_VALUE) {
                value = (V) values[i];
            }
        }

        getUpdateCallback().getProducer().send(new ProducerRecord<K, V>(getTable().getName(), key, value));
    }

}

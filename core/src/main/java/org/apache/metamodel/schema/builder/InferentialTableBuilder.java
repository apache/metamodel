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
package org.apache.metamodel.schema.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.DocumentSource;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;

/**
 * Implementation of {@link TableBuilder} that
 */
public class InferentialTableBuilder implements TableBuilder {

    private static final int MAX_SAMPLE_SIZE = 1000;

    private final String _tableName;
    private final Map<String, InferentialColumnBuilder> _columnBuilders;
    private final AtomicInteger _observationCounter;

    public InferentialTableBuilder(String tableName) {
        _tableName = tableName;
        _columnBuilders = new HashMap<String, InferentialColumnBuilder>();
        _observationCounter = new AtomicInteger();
    }

    public void addObservation(Document document) {
        _observationCounter.incrementAndGet();
        final Map<String, ?> values = document.getValues();
        final Set<? extends Entry<?, ?>> entries = values.entrySet();
        for (final Entry<?, ?> entry : entries) {
            final Object key = entry.getKey();
            if (key != null) {
                final String column = key.toString();
                final Object value = entry.getValue();
                final InferentialColumnBuilder columnBuilder = getColumnBuilder(column);
                columnBuilder.addObservation(value);
            }
        }
    }

    /**
     * Gets the number of observations that this table builder is basing it's
     * inference on.
     * 
     * @return
     */
    public int getObservationCount() {
        return _observationCounter.get();
    }

    @Override
    public MutableTable buildTable() {
        final int tableObservations = getObservationCount();

        // sort column names by copying them to a TreeSet
        final Set<String> columnNames = new TreeSet<String>(_columnBuilders.keySet());

        final MutableTable table = new MutableTable(_tableName);
        int columnNumber = 0;
        for (final String columnName : columnNames) {
            final InferentialColumnBuilder columnBuilder = getColumnBuilder(columnName);
            final MutableColumn column = columnBuilder.build();
            column.setTable(table);
            column.setColumnNumber(columnNumber);

            final int columnObservations = columnBuilder.getObservationCount();
            if (tableObservations > columnObservations) {
                // there may be nulls - some records does not even contain the
                // column
                column.setNullable(true);
            }

            table.addColumn(column);

            columnNumber++;
        }

        return table;
    }

    @Override
    public InferentialColumnBuilder getColumnBuilder(String columnName) {
        InferentialColumnBuilder columnBuilder = _columnBuilders.get(columnName);
        if (columnBuilder == null) {
            columnBuilder = new InferentialColumnBuilder(columnName);
            _columnBuilders.put(columnName, columnBuilder);
        }
        return columnBuilder;
    }

    @Override
    public void offerSource(DocumentSource documentSource) {
        while (getObservationCount() < MAX_SAMPLE_SIZE) {
            Document map = documentSource.next();
            if (map == null) {
                return;
            }
            addObservation(map);
        }
    }
}

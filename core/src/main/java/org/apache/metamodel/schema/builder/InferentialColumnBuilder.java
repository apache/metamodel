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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.schema.MutableColumn;

public class InferentialColumnBuilder implements ColumnBuilder {

    private final String _name;
    private final Set<ColumnType> _columnTypes;
    private final AtomicInteger _observationCounter;
    private boolean _nulls;

    public InferentialColumnBuilder(String columnName) {
        _name = columnName;
        _columnTypes = new HashSet<ColumnType>();
        _observationCounter = new AtomicInteger();
        _nulls = false;
    }

    public void addObservation(Object value) {
        _observationCounter.incrementAndGet();
        if (value == null) {
            _nulls = true;
            return;
        }
        final Class<? extends Object> valueType = value.getClass();
        addObservation(valueType);
    }

    public void addObservation(ColumnType columnType) {
        _observationCounter.incrementAndGet();
        if (columnType == null) {
            columnType = ColumnType.OTHER;
        }
        _columnTypes.add(columnType);
    }

    public void addObservation(Class<?> valueType) {
        final ColumnType columnType = ColumnTypeImpl.convertColumnType(valueType);
        addObservation(columnType);
    }

    /**
     * Gets the number of observations that this column builder is basing it's
     * inference on.
     * 
     * @return
     */
    public int getObservationCount() {
        return _observationCounter.get();
    }

    @Override
    public MutableColumn build() {
        final MutableColumn column = new MutableColumn(_name);
        column.setType(detectType());
        if (_nulls) {
            column.setNullable(true);
        }
        return column;
    }

    private ColumnType detectType() {
        if (_columnTypes.isEmpty()) {
            return ColumnType.OTHER;
        }

        if (_columnTypes.size() == 1) {
            return _columnTypes.iterator().next();
        }

        boolean allStrings = true;
        boolean allNumbers = true;

        for (ColumnType type : _columnTypes) {
            if (allStrings && !type.isLiteral()) {
                allStrings = false;
            } else if (allNumbers && !type.isNumber()) {
                allNumbers = false;
            }
        }

        if (allStrings) {
            return ColumnType.STRING;
        }

        if (allNumbers) {
            return ColumnType.NUMBER;
        }

        return ColumnType.OTHER;
    }
}

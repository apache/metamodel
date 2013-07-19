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
package org.apache.metamodel.convert;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DataSet} wrapper/decorator which converts values using
 * {@link TypeConverter}s before returning them to the user.
 */
final class ConvertedDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(ConvertedDataSet.class);

    private final DataSet _dataSet;
    private final TypeConverter<?, ?>[] _converters;

    public ConvertedDataSet(DataSet dataSet, TypeConverter<?, ?>[] converters) {
        super(dataSet.getSelectItems());
        _dataSet = dataSet;
        _converters = converters;
    }

    @Override
    public boolean next() {
        return _dataSet.next();
    }

    @Override
    public Row getRow() {
        Row sourceRow = _dataSet.getRow();
        Object[] values = new Object[_converters.length];
        for (int i = 0; i < values.length; i++) {
            Object value = sourceRow.getValue(i);

            @SuppressWarnings("unchecked")
            TypeConverter<Object, ?> converter = (TypeConverter<Object, ?>) _converters[i];

            if (converter != null) {
                Object virtualValue = converter.toVirtualValue(value);
                logger.debug("Converted physical value {} to {}", value, virtualValue);
                value = virtualValue;
            }
            values[i] = value;
        }
        return new DefaultRow(getHeader(), values);
    }

    @Override
    public void close() {
        _dataSet.close();
    }
}

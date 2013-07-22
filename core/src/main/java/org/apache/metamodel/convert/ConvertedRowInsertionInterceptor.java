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

import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.intercept.RowInsertionInterceptor;
import org.apache.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RowInsertionInterceptor} used for intercepting values in
 * {@link RowInsertionBuilder}s that need to be converted, according to a set of
 * {@link TypeConverter}s.
 * 
 * @see TypeConverter
 * @see Converters
 */
public class ConvertedRowInsertionInterceptor implements RowInsertionInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ConvertedRowInsertionInterceptor.class);

    private final Map<Column, TypeConverter<?, ?>> _converters;

    public ConvertedRowInsertionInterceptor() {
        this(new HashMap<Column, TypeConverter<?, ?>>());
    }

    public ConvertedRowInsertionInterceptor(Map<Column, TypeConverter<?, ?>> converters) {
        _converters = converters;
    }

    public void addConverter(Column column, TypeConverter<?, ?> converter) {
        if (converter == null) {
            _converters.remove(column);
        } else {
            _converters.put(column, converter);
        }
    }

    @Override
    public RowInsertionBuilder intercept(RowInsertionBuilder insert) {
        if (_converters.isEmpty()) {
            return insert;
        }

        logger.debug("Insert statement before conversion: {}", insert);

        insert = Converters.convertRow(insert, _converters);

        logger.debug("Insert statement after conversion:  {}", insert);

        return insert;
    }

}

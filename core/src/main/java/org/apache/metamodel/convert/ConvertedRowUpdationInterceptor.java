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

import org.apache.metamodel.intercept.RowUpdationInterceptor;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.update.RowUpdationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertedRowUpdationInterceptor implements RowUpdationInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ConvertedRowUpdationInterceptor.class);

    private final Map<Column, TypeConverter<?, ?>> _converters;

    public ConvertedRowUpdationInterceptor() {
        this(new HashMap<Column, TypeConverter<?, ?>>());
    }

    public ConvertedRowUpdationInterceptor(Map<Column, TypeConverter<?, ?>> converters) {
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
    public RowUpdationBuilder intercept(RowUpdationBuilder update) {
        if (_converters.isEmpty()) {
            return update;
        }

        logger.debug("Update statement after conversion:  {}", update);

        update = Converters.convertRow(update, _converters);
        
        logger.debug("Update statement after conversion:  {}", update);

        return update;
    }

}

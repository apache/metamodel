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

import java.util.Map;

import org.apache.metamodel.convert.DocumentConverter;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.util.CollectionUtils;

/**
 * Converter that assumes that keys in the documents are represented as columns
 * in a table.
 */
public class ColumnNameAsKeysRowConverter implements DocumentConverter {
    
    @Override
    public Row convert(Document document, DataSetHeader header) {
        final Object[] values = new Object[header.size()];
        for (int i = 0; i < values.length; i++) {
            final String columnName = header.getSelectItem(i).getColumn().getName();
            values[i] = get(document, columnName);
        }
        return new DefaultRow(header, values);
    }

    protected Object get(Document document, String columnName) {
        final Map<String, ?> map = document.getValues();
        final Object value = CollectionUtils.find(map, columnName);
        return value;
    }

}

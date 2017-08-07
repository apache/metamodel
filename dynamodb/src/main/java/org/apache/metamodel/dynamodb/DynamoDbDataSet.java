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
package org.apache.metamodel.dynamodb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

final class DynamoDbDataSet extends AbstractDataSet {

    private final Iterator<Map<String, AttributeValue>> _iterator;
    private Map<String, AttributeValue> _currentItem;

    public DynamoDbDataSet(List<Column> columns, ScanResult result) {
        super(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
        _iterator = result.getItems().iterator();
    }

    @Override
    public boolean next() {
        final boolean hasNext = _iterator.hasNext();
        if (hasNext) {
            _currentItem = _iterator.next();
            return true;
        }
        _currentItem = null;
        return false;
    }

    @Override
    public Row getRow() {
        if (_currentItem == null) {
            return null;
        }
        final DataSetHeader header = getHeader();
        final Object[] values = new Object[header.size()];
        for (int i = 0; i < values.length; i++) {
            final AttributeValue attributeValue = _currentItem.get(header.getSelectItem(i).getColumn().getName());
            values[i] = DynamoDbUtils.toValue(attributeValue);
        }
        final Row row = new DefaultRow(header, values);
        return row;
    }
}

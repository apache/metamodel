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
package org.apache.metamodel.couchdb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.StreamingViewResult;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;

/**
 * DataSet implementation for couch db.
 */
final class CouchDbDataSet extends AbstractDataSet {

    private final Iterator<org.ektorp.ViewResult.Row> _iterator;
    private final StreamingViewResult _streamingViewResult;
    private DefaultRow _row;

    public CouchDbDataSet(SelectItem[] selectItems, StreamingViewResult streamingViewResult) {
        super(selectItems);
        _streamingViewResult = streamingViewResult;

        _iterator = _streamingViewResult.iterator();
    }

    @Override
    public boolean next() {
        if (_iterator == null || !_iterator.hasNext()) {
            return false;
        }

        final org.ektorp.ViewResult.Row row = _iterator.next();
        final JsonNode node = row.getDocAsNode();
        final int size = getHeader().size();
        final Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            final String key = getHeader().getSelectItem(i).getColumn().getName();
            final JsonNode valueNode = node.get(key);
            final Object value;
            if (valueNode == null || valueNode.isNull()) {
                value = null;
            } else if (valueNode.isTextual()) {
                value = valueNode.asText();
            } else if (valueNode.isArray()) {
                value = toList(valueNode);
            } else if (valueNode.isObject()) {
                value = toMap(valueNode);
            } else if (valueNode.isBoolean()) {
                value = valueNode.asBoolean();
            } else if (valueNode.isInt()) {
                value = valueNode.asInt();
            } else if (valueNode.isLong()) {
                value = valueNode.asLong();
            } else if (valueNode.isDouble()) {
                value = valueNode.asDouble();
            } else {
                value = valueNode;
            }
            values[i] = value;
        }

        _row = new DefaultRow(getHeader(), values);

        return true;
    }

    private Map<String, Object> toMap(JsonNode valueNode) {
        if (valueNode == null) {
            return null;
        }
        try {
            return new ObjectMapper().reader(Map.class).readValue(valueNode);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Object toList(JsonNode valueNode) {
        if (valueNode == null) {
            return null;
        }
        try {
            return new ObjectMapper().reader(List.class).readValue(valueNode);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Row getRow() {
        return _row;
    }

    @Override
    public void close() {
        super.close();
        _streamingViewResult.close();
    }

}

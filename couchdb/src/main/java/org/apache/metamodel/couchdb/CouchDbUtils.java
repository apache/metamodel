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

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.util.CollectionUtils;
import org.ektorp.DbAccessException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Convenience and utility methods for MetaModel's CouchDB adaptor
 */
final class CouchDbUtils {
    
    private static final ObjectReader OBJECT_READER = new ObjectMapper().readerFor(Map.class);;

    /**
     * Safely calls hasNext on a row iterator
     * 
     * @param rowIterator
     * @return
     */
    public static boolean safeHasNext(Iterator<?> rowIterator) {
        try {
            return rowIterator.hasNext();
        } catch (DbAccessException e) {
            return false;
        }
    }

    /**
     * Converts {@link JsonNode} to MetaModel {@link Row}.
     * 
     * @param node
     *            {@link JsonNode} to convert.
     * @param selectItems
     *            Column names for the values in the row.
     * @return MetaModel {@link Row} populated with values from {@link JsonNode}
     *         .
     */
    public static Row jsonNodeToMetaModelRow(JsonNode node, DataSetHeader header) {
        final Map<String, ?> map = jsonNodeToMap(node);

        final int size = header.size();
        final Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            final String key = header.getSelectItem(i).getColumn().getName();
            final Object value = CollectionUtils.find(map, key);
            values[i] = value;
        }

        final DefaultRow finalRow = new DefaultRow(header, values);

        return finalRow;
    }

    /**
     * Converts {@link JsonNode} to a {@link Map}.
     * 
     * @param valueNode
     *            The {@link JsonNode} to convert.
     * @return The {@link Map} with values from {@link JsonNode}.
     */
    public static Map<String, ?> jsonNodeToMap(JsonNode valueNode) {
        if (valueNode == null) {
            return null;
        }
        try {
            return OBJECT_READER.readValue(valueNode);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Converts {@link JsonNode} to a {@link List}.
     * 
     * @param valueNode
     *            The {@link JsonNode} to convert.
     * @return The {@link List} with values from {@link JsonNode}.
     */
    public static Object jsonNodeToList(JsonNode valueNode) {
        if (valueNode == null) {
            return null;
        }
        try {
            return OBJECT_READER.readValue(valueNode);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

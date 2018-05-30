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
package org.apache.metamodel.neo4j;

import static org.apache.metamodel.neo4j.Neo4jDataContext.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.metamodel.schema.ColumnType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ColumnTypeResolver {
    private static final Logger logger = LoggerFactory.getLogger(ColumnTypeResolver.class);
    private final JSONObject _jsonObject;
    private final List<String> _columnNames = new ArrayList<>();

    public ColumnTypeResolver(final JSONObject jsonObject, final String[] columnNamesArray) {
        _jsonObject = jsonObject;
        _columnNames.addAll(Arrays.asList(columnNamesArray));
    }

    public ColumnType[] getColumnTypes() {
        final List<ColumnType> columnTypes = new ArrayList<>();

        try {
            columnTypes.addAll(getColumnTypesFromMetadata());
            columnTypes.addAll(getColumnTypesFromData());
        } catch (final JSONException e) {
            // ignore missing data
        }

        columnTypes.addAll(getColumnTypesFromRemainingColumns());
        return columnTypes.toArray(new ColumnType[columnTypes.size()]);
    }

    private List<ColumnType> getColumnTypesFromData() throws JSONException {
        final List<ColumnType> columnTypes = new ArrayList<>();
       
        if (_jsonObject.has(NEO4J_KEY_DATA)) {
            final JSONObject data = _jsonObject.getJSONObject(NEO4J_KEY_DATA);
            final Iterator<?> keysIterator = data.keys();

            while (keysIterator.hasNext()) {
                final String key = (String) keysIterator.next();
                final ColumnType type = getTypeFromValue(data, key);
                columnTypes.add(type);
                removeIfAvailable(_columnNames, key);
            }
        }

        return columnTypes;
    }

    private List<ColumnType> getColumnTypesFromMetadata() throws JSONException {
        final List<ColumnType> columnTypes = new ArrayList<>();

        if (_jsonObject.has(NEO4J_KEY_METADATA)) {
            final JSONObject metadata = _jsonObject.getJSONObject(NEO4J_KEY_METADATA);

            if (metadata.has(NEO4J_KEY_ID)) {
                columnTypes.add(ColumnType.BIGINT);
                removeIfAvailable(_columnNames, NEO4J_COLUMN_NAME_ID);
            }
        }

        return columnTypes;
    }

    private List<ColumnType> getColumnTypesFromRemainingColumns() {
        final List<ColumnType> columnTypes = new ArrayList<>();

        for (final String remainingColumnName : _columnNames) {
            if (remainingColumnName.contains(NEO4J_COLUMN_NAME_RELATION_PREFIX)) {
                if (remainingColumnName.contains(NEO4J_COLUMN_NAME_RELATION_LIST_INDICATOR)) {
                    columnTypes.add(ColumnType.LIST);
                } else {
                    columnTypes.add(ColumnType.BIGINT);
                }
            } else {
                columnTypes.add(ColumnType.STRING);
            }
        }

        return columnTypes;
    }

    private void removeIfAvailable(final List<String> list, final String key) {
        if (list.contains(key)) {
            list.remove(key);
        }
    }

    private ColumnType getTypeFromValue(final JSONObject data, final String key) {
        try {
            final Class<? extends Object> keyClass = data.get(key).getClass();

            if (keyClass.equals(Boolean.class)) {
                return ColumnType.BOOLEAN;
            } else if (keyClass.equals(Integer.class)) {
                return ColumnType.INTEGER;
            } else if (keyClass.equals(Long.class)) {
                return ColumnType.BIGINT;
            } else if (keyClass.equals(Double.class)) {
                return ColumnType.DOUBLE;
            } else if (keyClass.equals(JSONArray.class)) {
                return ColumnType.LIST;
            }
        } catch (final JSONException e) {
            logger.error("JSON object does not contain required key '{}'. {}", key, e.getMessage());
        }

        return ColumnType.STRING;
    }
}

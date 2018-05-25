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
package org.apache.metamodel.neo4j.utils;

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

public class ColumnTypeResolver {
    private static final Logger logger = LoggerFactory.getLogger(ColumnTypeResolver.class);
    private final JSONObject _jsonObject;
    private final List<String> _columnNames = new ArrayList<>();
    private final List<ColumnType> _columnTypes = new ArrayList<>();

    public ColumnTypeResolver(final JSONObject jsonObject, final String[] columnNamesArray) {
        _jsonObject = jsonObject;
        _columnNames.addAll(Arrays.asList(columnNamesArray));
    }
    
    public ColumnType[] getColumnTypes() {
        try {
            fillColumnTypesFromMetadata();
            fillColumnTypesFromData();
        } catch (final JSONException e) {
            // ignore missing data
        }

        fillColumnTypesFromRemainingColumns();
        return _columnTypes.toArray(new ColumnType[_columnTypes.size()]);
    }

    private void fillColumnTypesFromData() throws JSONException { 
        final String dataKey = "data";

        if (_jsonObject.has(dataKey)) {
            final JSONObject data = _jsonObject.getJSONObject(dataKey);
            final Iterator<?> keysIterator = data.keys();

            while (keysIterator.hasNext()) {
                final String key = (String) keysIterator.next();
                final ColumnType type = getTypeFromValue(data, key);
                _columnTypes.add(type);
                removeIfAvailable(_columnNames, key);
            }
        }
    }

    private void fillColumnTypesFromMetadata() throws JSONException {
        final String metadataKey = "metadata";

        if (_jsonObject.has(metadataKey)) {
            final JSONObject metadata = _jsonObject.getJSONObject(metadataKey);

            if (metadata.has("id")) {
                _columnTypes.add(ColumnType.BIGINT);
                removeIfAvailable(_columnNames, "_id");
            }
        }
    }

    private void fillColumnTypesFromRemainingColumns() {
        for (final String remainingColumnName : _columnNames) {
            if (remainingColumnName.contains("rel_")) {
                if (remainingColumnName.contains("#")) {
                    _columnTypes.add(ColumnType.LIST);
                } else {
                    _columnTypes.add(ColumnType.BIGINT);
                }
            } else {
                _columnTypes.add(ColumnType.STRING);
            }
        }
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

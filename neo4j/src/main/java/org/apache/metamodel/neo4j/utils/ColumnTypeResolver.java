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
import org.json.JSONException;
import org.json.JSONObject;

public class ColumnTypeResolver {
    public ColumnType[] getColumnTypes(final JSONObject jsonObject, final String[] columnNamesArray) {
        final List<String> columnNames = new ArrayList<>(Arrays.asList(columnNamesArray)); 
        final List<ColumnType> columnTypes = new ArrayList<>();

        try {
            fillColumnTypesFromMetadata(jsonObject, columnNames, columnTypes);
            fillColumnTypesFromData(jsonObject, columnNames, columnTypes);
        } catch (final JSONException e) {
            // ignore missing data
        }

        fillColumnTypesFromRemainingColumns(columnNames, columnTypes);
        return columnTypes.toArray(new ColumnType[columnTypes.size()]);
    }

    private void fillColumnTypesFromData(final JSONObject jsonObject, final List<String> columnNames,
            final List<ColumnType> columnTypes) throws JSONException {
        final String dataKey = "data";

        if (jsonObject.has(dataKey)) {
            final JSONObject data = jsonObject.getJSONObject(dataKey);
            final Iterator keysIterator = data.keys();

            while (keysIterator.hasNext()) {
                final String key = (String) keysIterator.next();
                final ColumnType type = getTypeFromValue(data, key);
                columnTypes.add(type);
                removeIfAvailable(columnNames, key);
            }
        }
    }

    private void fillColumnTypesFromMetadata(final JSONObject jsonObject, final List<String> columnNames,
            final List<ColumnType> columnTypes) throws JSONException {
        final String metadataKey = "metadata";

        if (jsonObject.has(metadataKey)) {
            final JSONObject metadata = jsonObject.getJSONObject(metadataKey);

            if (metadata.has("id")) {
                columnTypes.add(ColumnType.BIGINT);
                removeIfAvailable(columnNames, "_id");
            }
        }
    }

    private void fillColumnTypesFromRemainingColumns(final List<String> columnNames,
            final List<ColumnType> columnTypes) {
        for (final String remainingColumnName : columnNames) {
            if (remainingColumnName.contains("rel_")) {
                if (remainingColumnName.contains("#")) {
                    columnTypes.add(ColumnType.ARRAY);
                } else {
                    columnTypes.add(ColumnType.BIGINT);
                }
            } else {
                columnTypes.add(ColumnType.STRING);
            }
        }
    }

    private void removeIfAvailable(final List<String> list, final String key) {
        if (list.contains(key)) {
            list.remove(key);
        }
    }

    private ColumnType getTypeFromValue(final JSONObject data, final String key) {
        final BooleanColumnTypeHandler booleanHandler = new BooleanColumnTypeHandler();
        final IntegerColumnTypeHandler integerHandler = new IntegerColumnTypeHandler();
        final LongColumnTypeHandler longHandler = new LongColumnTypeHandler();
        final DoubleColumnTypeHandler doubleHandler = new DoubleColumnTypeHandler();
        final ArrayColumnTypeHandler arrayHandler = new ArrayColumnTypeHandler();
        final MapColumnTypeHandler mapHandler = new MapColumnTypeHandler();
        final StringColumnTypeHandler stringHandler = new StringColumnTypeHandler();

        // chain of responsibility
        booleanHandler.setSuccessor(integerHandler);
        integerHandler.setSuccessor(longHandler);
        longHandler.setSuccessor(doubleHandler);
        doubleHandler.setSuccessor(arrayHandler);
        arrayHandler.setSuccessor(mapHandler);
        mapHandler.setSuccessor(stringHandler);

        return booleanHandler.getTypeFromValue(data, key);
    }
}

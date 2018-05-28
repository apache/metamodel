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

import static junit.framework.TestCase.assertEquals;
import static org.apache.metamodel.neo4j.Neo4jDataContext.*;

import org.apache.metamodel.schema.ColumnType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

public class ColumnTypeResolverTest {
    private static final String COLUMN_BOOLEAN = "boolean";
    private static final String COLUMN_INTEGER = "integer";
    private static final String COLUMN_LONG = "long";
    private static final String COLUMN_DOUBLE = "double";
    private static final String COLUMN_ARRAY = "array";
    private static final String COLUMN_STRING = "string";

    @Test
    public void testGetColumnTypes() throws Exception {
        final JSONObject jsonObject = createJSONObject();
        final String[] columnNames =
                new String[] { NEO4J_COLUMN_NAME_ID, COLUMN_BOOLEAN, COLUMN_INTEGER, COLUMN_LONG, COLUMN_DOUBLE,
                        COLUMN_ARRAY, COLUMN_STRING };
        final ColumnTypeResolver resolver = new ColumnTypeResolver(jsonObject, columnNames);
        final ColumnType[] columnTypes = resolver.getColumnTypes();
        assertEquals(columnTypes.length, columnNames.length);
        assertEquals(columnTypes[0], ColumnType.BIGINT); // ID
        assertEquals(columnTypes[1], ColumnType.BOOLEAN);
        assertEquals(columnTypes[2], ColumnType.STRING);
        assertEquals(columnTypes[3], ColumnType.LIST);
        assertEquals(columnTypes[4], ColumnType.DOUBLE);
        assertEquals(columnTypes[5], ColumnType.INTEGER);
        assertEquals(columnTypes[6], ColumnType.BIGINT);
    }

    private JSONObject createJSONObject() throws JSONException {
        final JSONObject json = new JSONObject();
        final JSONObject metadata = new JSONObject();
        metadata.put(NEO4J_KEY_ID, 42L);
        json.put(NEO4J_KEY_METADATA, metadata);
        final JSONObject data = new JSONObject();
        data.put(COLUMN_BOOLEAN, true);
        data.put(COLUMN_STRING, "forty-two");
        final JSONArray array = new JSONArray();
        array.put(1).put(2).put(3);
        data.put(COLUMN_ARRAY, array);
        data.put(COLUMN_DOUBLE, 3.141592);
        data.put(COLUMN_INTEGER, 42);
        final JSONObject map = new JSONObject();
        map.put("1", "one").put("2", "two").put("3", "three");
        data.put(COLUMN_LONG, 12345678910L);
        json.put(NEO4J_KEY_DATA, data);

        return json;
    }
}

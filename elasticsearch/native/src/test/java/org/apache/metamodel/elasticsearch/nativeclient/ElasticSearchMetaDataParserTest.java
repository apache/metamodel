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
package org.apache.metamodel.elasticsearch.nativeclient;

import java.util.LinkedHashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.metamodel.elasticsearch.common.ElasticSearchMetaData;
import org.apache.metamodel.elasticsearch.common.ElasticSearchMetaDataParser;
import org.apache.metamodel.schema.ColumnType;
import org.elasticsearch.common.collect.MapBuilder;

public class ElasticSearchMetaDataParserTest extends TestCase {

    public void testParseMetadataInfo() throws Exception {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("message", MapBuilder.newMapBuilder().put("type", "long").immutableMap());
        metadata.put("postDate", MapBuilder.newMapBuilder().put("type", "date").put("format", "dateOptionalTime").immutableMap());
        metadata.put("anotherDate", MapBuilder.newMapBuilder().put("type", "date").put("format", "dateOptionalTime").immutableMap());
        metadata.put("user", MapBuilder.newMapBuilder().put("type", "string").immutableMap());
        metadata.put("critical", MapBuilder.newMapBuilder().put("type", "boolean").immutableMap());
        metadata.put("income", MapBuilder.newMapBuilder().put("type", "double").immutableMap());
        metadata.put("untypedthingie", MapBuilder.newMapBuilder().put("foo", "bar").immutableMap());
        
        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metadata);
        String[] columnNames = metaData.getColumnNames();
        ColumnType[] columnTypes = metaData.getColumnTypes();

        assertTrue(columnNames.length == 8);
        assertEquals(columnNames[0], "_id");
        assertEquals(columnNames[1], "message");
        assertEquals(columnNames[2], "postDate");
        assertEquals(columnNames[3], "anotherDate");
        assertEquals(columnNames[4], "user");
        assertEquals(columnNames[5], "critical");
        assertEquals(columnNames[6], "income");
        assertEquals(columnNames[7], "untypedthingie");
        
        assertTrue(columnTypes.length == 8);
        assertEquals(columnTypes[0], ColumnType.STRING);
        assertEquals(columnTypes[1], ColumnType.BIGINT);
        assertEquals(columnTypes[2], ColumnType.DATE);
        assertEquals(columnTypes[3], ColumnType.DATE);
        assertEquals(columnTypes[4], ColumnType.STRING);
        assertEquals(columnTypes[5], ColumnType.BOOLEAN);
        assertEquals(columnTypes[6], ColumnType.DOUBLE);
        assertEquals(columnTypes[7], ColumnType.STRING);
    }
}

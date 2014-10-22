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
package org.apache.metamodel.elasticsearch;

import junit.framework.TestCase;
import org.apache.metamodel.schema.ColumnType;

public class ElasticSearchMetaDataParserTest extends TestCase {

    public void testParseMetadataInfo() throws Exception {
        String metaDataInfo = "{message={type=long}, postDate={type=date, format=dateOptionalTime}, anotherDate={type=date, format=dateOptionalTime}, user={type=string}}";

        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metaDataInfo);
        String[] columnNames = metaData.getColumnNames();
        ColumnType[] columnTypes = metaData.getColumnTypes();

        assertTrue(columnNames.length==5);
        assertEquals(columnNames[0], "_id");
        assertEquals(columnNames[1], "message");
        assertEquals(columnNames[2], "postDate");
        assertEquals(columnNames[3], "anotherDate");
        assertEquals(columnNames[4], "user");
        assertTrue(columnTypes.length == 5);
        assertEquals(columnTypes[0], ColumnType.STRING);
        assertEquals(columnTypes[1], ColumnType.BIGINT);
        assertEquals(columnTypes[2], ColumnType.DATE);
        assertEquals(columnTypes[3], ColumnType.DATE);
        assertEquals(columnTypes[4], ColumnType.STRING);
    }
}

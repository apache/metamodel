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
package org.apache.metamodel.elasticsearch.common;

import junit.framework.TestCase;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;

import java.util.*;

public class ElasticSearchUtilsTest extends TestCase {

    public void testAssignDocumentIdForPrimaryKeys() throws Exception {
        MutableColumn primaryKeyColumn = new MutableColumn("value1", ColumnType.STRING).setPrimaryKey(true);
        SelectItem primaryKeyItem = new SelectItem(primaryKeyColumn);
        List<SelectItem> selectItems1 = Collections.singletonList(primaryKeyItem);
        String documentId = "doc1";
        DataSetHeader header = new SimpleDataSetHeader(selectItems1);
        Map<String, Object> values = new HashMap<>();
        values.put("value1", "theValue");
        Row row = ElasticSearchUtils.createRow(values, documentId, header);
        String primaryKeyValue = (String) row.getValue(primaryKeyItem);

        assertEquals(primaryKeyValue, documentId);
    }

    public void testCreateRowWithParsableDates() throws Exception {
        SelectItem item1 = new SelectItem(new MutableColumn("value1", ColumnType.STRING));
        SelectItem item2 = new SelectItem(new MutableColumn("value2", ColumnType.DATE));
        List<SelectItem> selectItems1 = Arrays.asList(item1, item2);
        String documentId = "doc1";
        DataSetHeader header = new SimpleDataSetHeader(selectItems1);
        Map<String, Object> values = new HashMap<>();
        values.put("value1", "theValue");
        values.put("value2", "2013-01-04T15:55:51.217+01:00");
        Row row = ElasticSearchUtils.createRow(values, documentId, header);
        Object stringValue = row.getValue(item1);
        Object dateValue = row.getValue(item2);

        assertTrue(stringValue instanceof String);
        assertTrue(dateValue instanceof Date);
    }
}

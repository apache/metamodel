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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;

import junit.framework.TestCase;

public class ElasticSearchUtilsTest extends TestCase {

    public void testAssignDocumentIdForPrimaryKeys() {
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

    public void testCreateRowWithParsableDates() {
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

    /**
     * For text-based conditions a 'match' query is recommended (instead of 'term' query).
     */
    public void testMatchQueryIsCreatedForTextEqualTo() {
        final SelectItem selectItem = new SelectItem(new MutableColumn("column_name", ColumnType.STRING));
        final FilterItem filterItem = new FilterItem(selectItem, OperatorType.EQUALS_TO, "text-value");
        final QueryBuilder queryBuilder =
                ElasticSearchUtils.createQueryBuilderForSimpleWhere(Collections.singletonList(filterItem), null);
        assertNotNull(queryBuilder);
        assertEquals("match", queryBuilder.getName());
    }

    /**
     * For text-based conditions a 'match' query is recommended (instead of 'term' query).
     * In case of 'DIFFERENT_FROM', we need a 'bool' query with 'must not' and 'match' query.
     */
    @SuppressWarnings("unchecked")
    public void testBoolQueryIsCreatedForTextDifferentFrom() {
        final SelectItem selectItem = new SelectItem(new MutableColumn("column_name", ColumnType.STRING));
        final FilterItem filterItem = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "text-value");
        final QueryBuilder queryBuilder =
                ElasticSearchUtils.createQueryBuilderForSimpleWhere(Collections.singletonList(filterItem), null);
        assertNotNull(queryBuilder);
        assertEquals("bool", queryBuilder.getName());
        final Map<String, Object> queryMap =
                XContentHelper.convertToMap(XContentType.JSON.xContent(), queryBuilder.toString(), false);
        final Map<String, Object> boolMap = (Map<String, Object>) queryMap.get("bool");
        assertNotNull(boolMap);
        assertTrue(boolMap.containsKey("must_not"));
        final List<Object> mustNotList = (List<Object>) boolMap.get("must_not");
        final Map<String, Object> mustNotMap = (Map<String, Object>) mustNotList.get(0);
        assertTrue(mustNotMap.containsKey("match"));
    }

    /**
     * For text-based conditions a 'match' query is recommended (instead of 'term' query).
     * To simulate 'IN' operator, we need a 'bool' query with multiple 'match' queries combined with 'OR'.
     */
    @SuppressWarnings("unchecked")
    public void testBoolQueryIsCreatedForTextIn() {
        final SelectItem selectItem = new SelectItem(new MutableColumn("column_name", ColumnType.STRING));
        final FilterItem filterItem =
                new FilterItem(selectItem, OperatorType.IN, Arrays.asList("text-value-a", "text-value-b"));
        final QueryBuilder queryBuilder =
                ElasticSearchUtils.createQueryBuilderForSimpleWhere(Collections.singletonList(filterItem), null);
        assertNotNull(queryBuilder);
        assertEquals("bool", queryBuilder.getName());
        final Map<String, Object> queryMap =
                XContentHelper.convertToMap(XContentType.JSON.xContent(), queryBuilder.toString(), false);
        final Map<String, Object> matchMap = (Map<String, Object>) queryMap.get("bool");
        assertNotNull(matchMap);
        assertTrue(matchMap.containsKey("should"));
        final List<Object> shouldList = (List<Object>) matchMap.get("should");
        assertNotNull(shouldList);
        assertEquals(2, shouldList.size());
        assertTrue(((Map<String, Object>)shouldList.get(0)).containsKey("match"));
        assertTrue(((Map<String, Object>)shouldList.get(1)).containsKey("match"));
    }
}

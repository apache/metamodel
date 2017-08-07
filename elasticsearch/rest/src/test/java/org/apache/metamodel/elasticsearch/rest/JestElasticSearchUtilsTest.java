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
package org.apache.metamodel.elasticsearch.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class JestElasticSearchUtilsTest {

    @Test
    public void testAssignDocumentIdForPrimaryKeys() throws Exception {
        MutableColumn primaryKeyColumn = new MutableColumn("value1", ColumnType.STRING).setPrimaryKey(true);
        SelectItem primaryKeyItem = new SelectItem(primaryKeyColumn);
        List<SelectItem> selectItems1 = Collections.singletonList(primaryKeyItem);
        String documentId = "doc1";
        DataSetHeader header = new SimpleDataSetHeader(selectItems1);
        JsonObject values = new JsonObject();

        values.addProperty("value1", "theValue");
        Row row = JestElasticSearchUtils.createRow(values, documentId, header);
        String primaryKeyValue = (String) row.getValue(primaryKeyItem);

        assertEquals(primaryKeyValue, documentId);
    }

    @Test
    public void testCreateRowWithNullValues() throws Exception {
        final Column col1 = new MutableColumn("col1", ColumnType.STRING);
        final Column col2 = new MutableColumn("col2", ColumnType.STRING);
        final DataSetHeader header = new SimpleDataSetHeader(Lists.newArrayList(col1, col2).stream().map(SelectItem::new).collect(Collectors.toList()));
        final JsonObject source = new JsonObject();
        source.addProperty("col1", "foo");
        source.addProperty("col2", (String) null);
        final String documentId = "row1";

        final Row row = JestElasticSearchUtils.createRow(source, documentId, header);
        assertEquals("Row[values=[foo, null]]", row.toString());
    }

    @Test
    public void testCreateRowWithNumberValueAndStringType() throws Exception {
        final Column col1 = new MutableColumn("col1", ColumnType.STRING);
        final DataSetHeader header =  SimpleDataSetHeader.fromColumns(Lists.newArrayList(col1));
        final JsonObject source = new JsonObject();
        source.addProperty("col1", 42);
        final String documentId = "row1";

        final Row row = JestElasticSearchUtils.createRow(source, documentId, header);
        assertEquals("Row[values=[42]]", row.toString());
    }

    @Test
    public void testCreateRowWithStringValueAndNumberType() throws Exception {
        final Column col1 = new MutableColumn("col1", ColumnType.NUMBER);
        final DataSetHeader header = SimpleDataSetHeader.fromColumns(Lists.newArrayList(col1));
        final JsonObject source = new JsonObject();
        source.addProperty("col1", "hello world");
        final String documentId = "row1";

        final Row row = JestElasticSearchUtils.createRow(source, documentId, header);

        // whether or not 'null' should be returned (bad value, but preserves
        // type) or 'hello world' should be returned (correct value, breaks
        // type) can be debated. For now it is added here as an assertion to
        // keep track of any regressions.
        assertEquals("Row[values=[null]]", row.toString());
    }

    @Test
    public void testCreateRowWithJsonObject() throws Exception {
        final Column col1 = new MutableColumn("col1", ColumnType.MAP);
        final DataSetHeader header = SimpleDataSetHeader.fromColumns(Lists.newArrayList(col1));
        final JsonObject source = new JsonObject();
        final JsonObject value = new JsonObject();
        value.addProperty("foo1", "bar");
        value.addProperty("foo2", 42);
        source.add("col1", value);
        final String documentId = "row1";

        final Row row = JestElasticSearchUtils.createRow(source, documentId, header);
        assertEquals("Row[values=[{foo1=bar, foo2=42.0}]]", row.toString());

        final Map<?, ?> rowValue = (Map<?, ?>) row.getValue(col1);
        assertEquals("bar", rowValue.get("foo1"));
    }

    @Test
    public void testCreateRowWithJsonArray() throws Exception {
        final Column col1 = new MutableColumn("col1", ColumnType.LIST);
        final DataSetHeader header = SimpleDataSetHeader.fromColumns(Lists.newArrayList(col1));
        final JsonObject source = new JsonObject();
        final JsonArray value = new JsonArray();
        value.add(new JsonPrimitive("foo"));
        value.add(new JsonPrimitive("bar"));
        source.add("col1", value);
        final String documentId = "row1";

        final Row row = JestElasticSearchUtils.createRow(source, documentId, header);
        assertEquals("Row[values=[[foo, bar]]]", row.toString());

        final List<?> rowValue = (List<?>) row.getValue(col1);
        assertEquals("foo", rowValue.get(0));
    }

    @Test
    public void testCreateRowWithDeepNesting() throws Exception {
        final Column col1 = new MutableColumn("col1", ColumnType.LIST);
        final DataSetHeader header = SimpleDataSetHeader.fromColumns(Lists.newArrayList(col1));
        final JsonObject source = new JsonObject();

        final JsonObject obj2 = new JsonObject();
        obj2.addProperty("foo", 43);

        final JsonArray arr1 = new JsonArray();
        arr1.add(new JsonPrimitive("foo"));
        arr1.add(new JsonPrimitive("bar"));
        arr1.add(obj2);

        final JsonObject obj1 = new JsonObject();
        obj1.addProperty("mybool", true);
        obj1.add("arr1", arr1);
        source.add("col1", obj1);
        final String documentId = "row1";

        final Row row = JestElasticSearchUtils.createRow(source, documentId, header);
        assertEquals("Row[values=[{mybool=true, arr1=[foo, bar, {foo=43.0}]}]]", row.toString());

        final Map<?, ?> rowObj1 = (Map<?, ?>) row.getValue(col1);
        final List<?> rowList = (List<?>) rowObj1.get("arr1");
        final Map<?, ?> rowObj2 = (Map<?, ?>) rowList.get(2);
        assertEquals(43.0, rowObj2.get("foo"));
    }

    @Test
    public void testCreateRowWithParseableDates() throws Exception {
        SelectItem item1 = new SelectItem(new MutableColumn("value1", ColumnType.STRING));
        SelectItem item2 = new SelectItem(new MutableColumn("value2", ColumnType.DATE));
        List<SelectItem> selectItems1 = Arrays.asList(item1, item2);
        String documentId = "doc1";
        DataSetHeader header = new SimpleDataSetHeader(selectItems1);
        JsonObject values = new JsonObject();
        values.addProperty("value1", "theValue");
        values.addProperty("value2", "2013-01-04T15:55:51.217+01:00");
        Row row = JestElasticSearchUtils.createRow(values, documentId, header);
        Object stringValue = row.getValue(item1);
        Object dateValue = row.getValue(item2);

        assertTrue(stringValue instanceof String);
        assertTrue(dateValue instanceof Date);
    }
}

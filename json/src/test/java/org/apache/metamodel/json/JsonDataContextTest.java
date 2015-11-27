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
package org.apache.metamodel.json;

import java.io.File;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.builder.SchemaBuilder;
import org.apache.metamodel.schema.builder.SimpleTableDefSchemaBuilder;
import org.apache.metamodel.schema.builder.SingleMapColumnSchemaBuilder;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.SimpleTableDef;

public class JsonDataContextTest extends TestCase {

    public void testReadArrayWithDocumentsFile() throws Exception {
        final JsonDataContext dc = new JsonDataContext(new File("src/test/resources/array_with_documents.json"));
        runParseabilityTest(dc);
    }

    public void testDocumentsOnEveryLineFile() throws Exception {
        final JsonDataContext dc = new JsonDataContext(new File("src/test/resources/documents_on_every_line.json"));
        runParseabilityTest(dc);
    }

    public void testUseAnotherSchemaBuilder() throws Exception {
        final SchemaBuilder schemaBuilder = new SingleMapColumnSchemaBuilder("sch", "tbl", "cl");
        final JsonDataContext dc = new JsonDataContext(new FileResource(
                "src/test/resources/documents_on_every_line.json"), schemaBuilder);

        final Table table = dc.getDefaultSchema().getTable(0);
        assertEquals("tbl", table.getName());

        assertEquals("[cl]", Arrays.toString(table.getColumnNames()));

        final DataSet dataSet = dc.query().from("tbl").select("cl").execute();
        assertTrue(dataSet.next());
        assertEquals("Row[values=[{id=1234, name=John Doe, country=US}]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[{id=1235, name=Jane Doe, country=USA, gender=F}]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    private void runParseabilityTest(JsonDataContext dc) {
        final Table table = dc.getDefaultSchema().getTable(0);
        assertEquals("[country, gender, id, name]", Arrays.toString(table.getColumnNames()));
        final Column[] columns = table.getColumns();

        final DataSet dataSet = dc.materializeMainSchemaTable(table, columns, 100000);
        assertTrue(dataSet.next());
        assertEquals("Row[values=[US, null, 1234, John Doe]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[USA, F, 1235, Jane Doe]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
        dataSet.close();
    }

    public void testUseCustomTableDefWithNestedColumnDefinition() throws Exception {
        final SimpleTableDef tableDef = new SimpleTableDef("mytable", new String[] { "name.first", "name.last",
                "gender", "interests[0]", "interests[0].type", "interests[0].name" });
        final Resource resource = new FileResource("src/test/resources/nested_fields.json");
        final SchemaBuilder schemaBuilder = new SimpleTableDefSchemaBuilder("myschema", tableDef);
        final JsonDataContext dataContext = new JsonDataContext(resource, schemaBuilder);

        final DataSet ds = dataContext.query().from("mytable").selectAll().execute();
        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[John, Doe, MALE, football, null, null]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[John, Doe, MALE, {type=sport, name=soccer}, sport, soccer]]", ds.getRow()
                    .toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }

    public void testUseMapValueFunctionToGetFromNestedMap() throws Exception {
        final Resource resource = new FileResource("src/test/resources/nested_fields.json");
        final JsonDataContext dataContext = new JsonDataContext(resource);

        final Schema schema = dataContext.getDefaultSchema();
        assertEquals("[nested_fields.json]", Arrays.toString(schema.getTableNames()));

        final DataSet ds = dataContext.query().from(schema.getTable(0))
                .select(FunctionType.MAP_VALUE, "name", new Object[] { "first" }).execute();
        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[John]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[John]]", ds.getRow().toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }

    public void testUseDotNotationToGetFromNestedMap() throws Exception {
        final Resource resource = new FileResource("src/test/resources/nested_fields.json");
        final JsonDataContext dataContext = new JsonDataContext(resource);

        final Schema schema = dataContext.getDefaultSchema();
        assertEquals("[nested_fields.json]", Arrays.toString(schema.getTableNames()));

        final DataSet ds = dataContext.query().from(schema.getTable(0)).select("name.first").execute();
        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[John]]", ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals("Row[values=[John]]", ds.getRow().toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }
    }
}

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
package org.apache.metamodel.query.builder;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MockDataContext;
import org.apache.metamodel.query.FunctionType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Table;

public class SyntaxExamplesTest extends TestCase {

    private DataContext dc;
    private Table table1;
    private Table table2;
    private Column col1;
    private Column col2;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        dc = new MockDataContext("sch", "tab1", "foo");
        MutableSchema schema = (MutableSchema) dc.getDefaultSchema();
        table1 = schema.getTables()[0];
        schema.addTable(new MutableTable("tab2").setSchema(schema));
        table2 = schema.getTableByName("tab2");
        col1 = table1.getColumns()[0];
        col2 = table1.getColumns()[1];
    }

    public void testSchema() throws Exception {
        assertEquals("tab1", table1.getName());
        assertEquals("sch.tab1", table1.getQualifiedLabel());
    }

    public void testFromAlias() throws Exception {
        dc.query().from(table1).as("t");
    }

    public void testFromJoin() throws Exception {
        dc.query().from(table1).innerJoin(table2).on(col1, col2).select(col1);
    }

    public void testWhereOr() throws Exception {
        dc.query().from(table1).as("t").select(col2).where(col1).isNotNull().or(col1).isNull().orderBy(col1).asc();
    }

    public void testGroupBy() throws Exception {
        dc.query().from(table1).selectCount().select(col1).groupBy(col1).having(FunctionType.SUM, col1).greaterThan(3)
                .orderBy(col1).asc();
    }
    
    public void testMultiGroupByAndHaving() throws Exception {
        dc.query().from(table1).select("foo", "bar", "COUNT(*)").groupBy("foo","bar").having("COUNT(*)").greaterThan(3)
                .orderBy(col1).asc();
    }

    public void testMultipleTables() throws Exception {
        Query q = dc.query().from(table1).as("t1").and(table2).as("t2").select(col1).where(col1).greaterThan(col2)
                .orderBy(col2).desc().toQuery();
        assertEquals("SELECT t1.foo FROM sch.tab1 t1, sch.tab2 t2 " + "WHERE t1.foo > t1.bar ORDER BY t1.bar DESC",
                q.toSql());
    }
}
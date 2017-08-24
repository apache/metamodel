/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.metamodel.jdbc.dialects;

import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_ORACLE;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OracleQueryRewriterTest {
    private MutableTable table;
    private MutableColumn column;
    private JdbcDataContext mockContext;
    private IQueryRewriter qr;

    @Before
    public void setUp() throws Exception {
        table = new MutableTable("foo");
        table.setSchema(new MutableSchema("MY_SCHEMA"));
        table.setQuote("\"");

        column = new MutableColumn("bar");
        column.setQuote("\"");
        column.setTable(table);

        mockContext = EasyMock.createMock(JdbcDataContext.class);
        setMetaData(DATABASE_PRODUCT_ORACLE, "R12.1.1.1");
        qr = new OracleQueryRewriter(mockContext);
    }

    @Test
    public void testReplaceEmptyStringWithNull() throws Exception {
        final String alias = "alias";
        SelectItem selectItem = new SelectItem("expression", alias);
        final FilterItem filterItem = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "");
        final String rewrittenValue = qr.rewriteFilterItem(filterItem);
        final String expectedValue = alias + " IS NOT NULL";

        assertEquals(expectedValue, rewrittenValue);
    }

    @Test
    public void testOffsetFetchConstruct() {
        final int offset = 1000;
        final int rows = 100;
        final String where = "x > 1";

        final String offsetClause = " OFFSET " + (offset - 1) + " ROWS";
        final String fetchClause = " FETCH NEXT " + rows + " ROWS ONLY";

        final Query query = new Query().from(table).select(column);
        Assert.assertEquals("There shouldn't be OFFSET-FETCH clause.", query.toSql(), qr.rewriteQuery(query));

        query.setFirstRow(offset);
        Assert.assertEquals("Wrong or missing OFFSET clause.", query.toSql() + offsetClause, qr.rewriteQuery(query));

        query.setMaxRows(rows);
        Assert.assertEquals("Wrong or missing OFFSET and FETCH clauses.", query.toSql() + offsetClause + fetchClause,
                qr.rewriteQuery(query));

        query.setFirstRow(null);
        Assert.assertEquals("Wrong or missing FETCH clause.", query.toSql() + fetchClause, qr.rewriteQuery(query));
    }

    @Test
    public void testOffsetFetchVersionCheck() throws SQLException {
        setMetaData(DATABASE_PRODUCT_ORACLE, "10.1.1.1");

        Query query = new Query().from(table).select(column).setFirstRow(1000).setMaxRows(100);
        Assert.assertEquals("The query shouldn't be rewritten.", query.toSql(), qr.rewriteQuery(query));
    }

    @Test
    public void testOffsetFetchVersionIsNull() throws SQLException {
        setMetaData(DATABASE_PRODUCT_ORACLE, null);

        Query query = new Query().from(table).select(column).setFirstRow(1000).setMaxRows(100);
        Assert.assertEquals("The query shouldn't be rewritten.", query.toSql(), qr.rewriteQuery(query));
    }

    private void setMetaData(String productName, String version) throws SQLException {
        EasyMock.reset(mockContext);

        EasyMock.expect(mockContext.getDatabaseProductName()).andReturn(productName).anyTimes();
        EasyMock.expect(mockContext.getDatabaseVersion()).andReturn(version).anyTimes();
        EasyMock.expect(mockContext.getIdentifierQuoteString()).andReturn("quoteString").anyTimes();

        EasyMock.replay(mockContext);
    }
}
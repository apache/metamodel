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
package org.apache.metamodel.dialects;

import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_SQLSERVER;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.dialects.SQLServerQueryRewriter;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.util.TimeComparator;
import org.easymock.EasyMock;
import org.junit.Assert;

import junit.framework.TestCase;

public class SQLServerQueryRewriterTest extends TestCase {

    private MutableTable table;
    private MutableColumn column;
    private SQLServerQueryRewriter qr;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        table = new MutableTable("foo");
        table.setSchema(new MutableSchema("MY_SCHEMA"));
        table.setQuote("\"");
        column = new MutableColumn("bar");
        column.setQuote("\"");
        column.setTable(table);

        final JdbcDataContext mockContext = EasyMock.createMock(JdbcDataContext.class);
        EasyMock.expect(mockContext.getDatabaseProductName()).andReturn(DATABASE_PRODUCT_SQLSERVER).anyTimes();
        EasyMock.expect(mockContext.getDatabaseVersion()).andReturn("12.1.1.1").anyTimes();
        EasyMock.expect(mockContext.getIdentifierQuoteString()).andReturn("quoteString").anyTimes();

        EasyMock.replay(mockContext);
        qr = new SQLServerQueryRewriter(mockContext);
    }

    public void testRewriteColumnTypeDouble() throws Exception {
        assertEquals("FLOAT", qr.rewriteColumnType(ColumnType.DOUBLE, null));
    }

    public void testRewriteColumnTypeVarchar() throws Exception {
        assertEquals("VARCHAR(128)", qr.rewriteColumnType(ColumnType.VARCHAR, 128));
        assertEquals("VARCHAR(MAX)", qr.rewriteColumnType(ColumnType.VARCHAR, null));
    }

    public void testRewriteFromItem() throws Exception {
        assertEquals("foo", qr.rewriteFromItem(new FromItem(new MutableTable("foo"))));
    }

    public void testAliasing() throws Exception {
        Query q = new Query().from(table).select(column);

        assertEquals("SELECT MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\"", qr.rewriteQuery(q));
    }

    public void testSelectMaxRowsRewriting() throws Exception {
        Query q = new Query().from(table).select(column).setMaxRows(20);

        assertEquals("SELECT TOP 20 MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\"", qr.rewriteQuery(q));
    }

    public void testOffsetFetchConstruct() {
        final int offset = 1000;
        final int rows = 100;

        final String baseQuery = "SELECT MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\" ORDER BY id ASC";
        final String baseQueryWithTop =
                "SELECT TOP " + rows + " MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\" ORDER BY id ASC";
        final String offsetClause = " OFFSET " + (offset - 1) + " ROWS";
        final String fetchClause = " FETCH NEXT " + rows + " ROWS ONLY";

        Query query = new Query();
        query.from(table).select(column).orderBy("id");
        Assert.assertEquals("There shouldn't be OFFSET-FETCH clause.", baseQuery, qr.rewriteQuery(query));

        query.setFirstRow(offset);
        Assert.assertEquals("Wrong or missing OFFSET clause.", baseQuery + offsetClause, qr.rewriteQuery(query));

        query.setMaxRows(rows);
        Assert.assertEquals("Wrong or missing OFFSET and FETCH clauses.", baseQuery + offsetClause + fetchClause,
                qr.rewriteQuery(query));

        query.setFirstRow(null);
        Assert.assertEquals("Using FETCH clause instead of TOP clause.", baseQueryWithTop, qr.rewriteQuery(query));
    }

    public void testRewriteFilterItem() {

        MutableColumn timestampColumn = new MutableColumn("timestamp");
        timestampColumn.setType(ColumnType.TIMESTAMP);
        timestampColumn.setNativeType("DATETIME");
        Query q = new Query()
                .from(table)
                .select(column)
                .select(timestampColumn)
                .where(new FilterItem(new SelectItem(timestampColumn), OperatorType.LESS_THAN, TimeComparator
                        .toDate("2014-06-28 14:06:00")));

        assertEquals(
                "SELECT MY_SCHEMA.\"foo\".\"bar\", timestamp FROM MY_SCHEMA.\"foo\" WHERE timestamp < CAST('20140628 14:06:00' AS DATETIME)",
                qr.rewriteQuery(q));
    }

    public void testSelectMaxRowsWithDistinctRewriting() throws Exception {
        Query q = new Query().from(table).selectDistinct().select(column).setMaxRows(20);
        assertEquals("SELECT DISTINCT TOP 20 MY_SCHEMA.\"foo\".\"bar\" FROM MY_SCHEMA.\"foo\"", qr.rewriteQuery(q));
    }
}
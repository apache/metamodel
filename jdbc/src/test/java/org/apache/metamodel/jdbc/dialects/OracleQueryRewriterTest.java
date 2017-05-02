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
package org.apache.metamodel.jdbc.dialects;

import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_ORACLE;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OracleQueryRewriterTest {

    private static final JdbcDataContext mockContext = EasyMock.createMock(JdbcDataContext.class);

    @BeforeClass
    public static void initMocks() throws SQLException {
        setMetaData(DATABASE_PRODUCT_ORACLE, "R12.1.1.1");
    }

    @Test
    public void testReplaceEmptyStringWithNull() throws Exception {
        final OracleQueryRewriter rewriter = new OracleQueryRewriter(mockContext);
        final String alias = "alias";
        SelectItem selectItem = new SelectItem("expression", alias);
        final FilterItem filterItem = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "");
        final String rewrittenValue = rewriter.rewriteFilterItem(filterItem);
        final String expectedValue = alias + " IS NOT NULL";
        
        assertEquals(expectedValue, rewrittenValue);
    }

    @Test
    public void testOffsetFetchConstruct() {
        final OracleQueryRewriter rewriter = new OracleQueryRewriter(mockContext);
        final int offset = 1000;
        final int rows = 100;
        final String table = "table";
        final String where = "x > 1";

        Query query = new Query();
        query.from(table).orderBy("id");
        final String queryWithoutBoth = query.toSql();
        Assert.assertEquals("Original SQL is not correctly generated.", " FROM table ORDER BY id ASC", queryWithoutBoth);
        final String queryWithoutBothRewritten = rewriter.rewriteQuery(query);
        Assert.assertEquals("There shouldn't be OFFSET-FETCH clause.", queryWithoutBoth, queryWithoutBothRewritten);

        query.setFirstRow(offset);
        final String queryWithoutMax = query.toSql();
        Assert.assertEquals("Original SQL is not correctly generated.", " FROM table ORDER BY id ASC", queryWithoutMax);
        final String queryWithoutMaxRewritten = rewriter.rewriteQuery(query);
        Assert.assertEquals("There shouldn't be OFFSET-FETCH clause.", queryWithoutMax, queryWithoutMaxRewritten);

        query.setMaxRows(rows).where(where);
        final String originalQuery = query.toSql();
        Assert.assertEquals("Original SQL is not correctly generated.", " FROM table WHERE x > 1 ORDER BY id ASC", originalQuery);

        String rewrittenQuery = rewriter.rewriteQuery(query);
        final String offsetFetchClause = " OFFSET " + (offset-1) + " ROWS FETCH NEXT " + rows + " ROWS ONLY";
        Assert.assertEquals("Not correctly generated Offset Fetch clouse.", originalQuery + offsetFetchClause, rewrittenQuery);
    }

    @Test
    public void testOffsetFetchVersionCheck() throws SQLException {
        setMetaData(DATABASE_PRODUCT_ORACLE, "10.1.1.1");

        final int offset = 1000;
        final int rows = 100;
        final String table = "table";

        Query query = new Query();
        query.from(table).setFirstRow(offset).setMaxRows(rows);
        final String originalQuery = query.toSql();
        Assert.assertEquals("Original SQL is not correctly generated.", " FROM table", originalQuery);

        final OracleQueryRewriter rewriter = new OracleQueryRewriter(mockContext);
        String rewrittenQuery = rewriter.rewriteQuery(query);
        Assert.assertEquals("The query shouldn't be rewritten.", originalQuery, rewrittenQuery);
    }

    @Test
    public void testOffsetFetchVersionIsNull() throws SQLException {
        setMetaData(DATABASE_PRODUCT_ORACLE, null);

        final int offset = 1000;
        final int rows = 100;
        final String table = "table";

        Query query = new Query();
        query.from(table).setFirstRow(offset).setMaxRows(rows);
        final String originalQuery = query.toSql();
        Assert.assertEquals("Original SQL is not correctly generated.", " FROM table", originalQuery);

        final OracleQueryRewriter rewriter = new OracleQueryRewriter(mockContext);
        String rewrittenQuery = rewriter.rewriteQuery(query);
        Assert.assertEquals("The query shouldn't be rewritten.", originalQuery, rewrittenQuery);
    }

    private static void setMetaData(String productName, String version) throws SQLException {
        EasyMock.reset(mockContext);

        EasyMock.expect(mockContext.getDatabaseProductName()).andReturn(productName).anyTimes();
        EasyMock.expect(mockContext.getDatabaseVersion()).andReturn(version).anyTimes();
        EasyMock.expect(mockContext.getIdentifierQuoteString()).andReturn("quoteString").anyTimes();

        EasyMock.replay(mockContext);
    }
}
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

import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_HIVE;
import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_SQLSERVER;

import junit.framework.TestCase;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.dialects.HiveQueryRewriter;
import org.apache.metamodel.jdbc.dialects.MysqlQueryRewriter;
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

public class HiveQueryRewriterTest extends TestCase {


    @Override
    protected void setUp() throws Exception {
        super.setUp();


    }

    public void testHive1SqlWithPagination() {
        final JdbcDataContext mockContext = EasyMock.createMock(JdbcDataContext.class);
        EasyMock.expect(mockContext.getDatabaseProductName()).andReturn(DATABASE_PRODUCT_HIVE).anyTimes();
        EasyMock.expect(mockContext.getDatabaseVersion()).andReturn("1.1.1.1").anyTimes();
        EasyMock.expect(mockContext.getIdentifierQuoteString()).andReturn("quoteString").anyTimes();

        EasyMock.replay(mockContext);
        HiveQueryRewriter qr = new HiveQueryRewriter(mockContext);

        MutableColumn col1 = new MutableColumn("kkbh");
        MutableColumn col2 = new MutableColumn("kkmc");
        Query q = new Query().from(new MutableTable("5_t_kk_kkxx")).select(col1).select(col2)
                .where(col1, OperatorType.EQUALS_TO, "5207281832").orderBy(col1).setFirstRow(5).setMaxRows(9);
        String sql = qr.rewriteQuery(q);
        assertEquals(sql,"SELECT metamodel_subquery.kkbh, metamodel_subquery.kkmc FROM (SELECT kkbh, kkmc, ROW_NUMBER() OVER( ORDER BY kkbh ASC) AS metamodel_row_number FROM 5_t_kk_kkxx WHERE kkbh = '5207281832') metamodel_subquery WHERE metamodel_row_number BETWEEN 5 AND 13");
    }

    public void testHive2SqlWithPagination() {
        final JdbcDataContext mockContext = EasyMock.createMock(JdbcDataContext.class);
        EasyMock.expect(mockContext.getDatabaseProductName()).andReturn(DATABASE_PRODUCT_HIVE).anyTimes();
        EasyMock.expect(mockContext.getDatabaseVersion()).andReturn("2.1.1.1").anyTimes();
        EasyMock.expect(mockContext.getIdentifierQuoteString()).andReturn("quoteString").anyTimes();

        EasyMock.replay(mockContext);
        HiveQueryRewriter qr = new HiveQueryRewriter(mockContext);

        MutableColumn col1 = new MutableColumn("kkbh");
        MutableColumn col2 = new MutableColumn("kkmc");
        Query q = new Query().from(new MutableTable("5_t_kk_kkxx")).select(col1).select(col2)
                .where(col1, OperatorType.EQUALS_TO, "5207281832").orderBy(col1).setFirstRow(5).setMaxRows(9);
        String sql = qr.rewriteQuery(q);
        assertEquals(sql,"SELECT kkbh, kkmc FROM 5_t_kk_kkxx WHERE kkbh = '5207281832' ORDER BY kkbh ASC LIMIT 9 OFFSET 4");
    }
}
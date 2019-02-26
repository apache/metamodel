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

import static junit.framework.TestCase.assertEquals;
import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_HIVE;
import static org.apache.metamodel.jdbc.JdbcDataContext.DATABASE_PRODUCT_IMPALA;

import junit.framework.TestCase;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.jdbc.dialects.Hive2QueryRewriter;
import org.apache.metamodel.jdbc.dialects.ImpalaQueryRewriter;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.easymock.EasyMock;

public class ImpalaQueryRewriterTest extends TestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testImpalaQuery() {

        final JdbcDataContext mockContext = EasyMock.createMock(JdbcDataContext.class);
        EasyMock.expect(mockContext.getDatabaseProductName()).andReturn(DATABASE_PRODUCT_IMPALA).anyTimes();
        EasyMock.expect(mockContext.getDatabaseVersion()).andReturn("3.0.0").anyTimes();
        EasyMock.expect(mockContext.getIdentifierQuoteString()).andReturn("quoteString").anyTimes();

        EasyMock.replay(mockContext);
        ImpalaQueryRewriter qr = new ImpalaQueryRewriter(mockContext);

        MutableColumn col1 = new MutableColumn("kkbh");
        MutableColumn col2 = new MutableColumn("kkmc");
        Query q = new Query().from(new MutableTable("5_t_kk_kkxx")).select(col1).select(col2)
                .where(col1, OperatorType.EQUALS_TO, "5207281832").orderBy(col1).setFirstRow(5).setMaxRows(9);
        String sql = qr.rewriteQuery(q);

        assertEquals(sql,"SELECT kkbh, kkmc FROM 5_t_kk_kkxx WHERE kkbh = '5207281832' ORDER BY kkbh ASC LIMIT 9 OFFSET 4");
    }
}

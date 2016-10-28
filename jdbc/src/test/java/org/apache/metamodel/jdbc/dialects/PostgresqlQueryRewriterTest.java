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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.easymock.EasyMock;
import org.junit.Test;

public class PostgresqlQueryRewriterTest {

    @Test
    public void testInsertNullMap() throws SQLException {
        final PreparedStatement statementMock = EasyMock.createMock(PreparedStatement.class);

        final PostgresqlQueryRewriter queryRewriter = new PostgresqlQueryRewriter(null);
        final Column column = new MutableColumn("col").setType(ColumnType.MAP).setNativeType("jsonb");
        final Object value = null;

        // mock behaviour recording
        statementMock.setObject(0, null);

        EasyMock.replay(statementMock);

        queryRewriter.setStatementParameter(statementMock, 0, column, value);

        EasyMock.verify(statementMock);
    }
}

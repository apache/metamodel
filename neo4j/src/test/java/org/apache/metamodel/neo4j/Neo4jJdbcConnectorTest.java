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
package org.apache.metamodel.neo4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

public class Neo4jJdbcConnectorTest extends Neo4jTestCase {

    @Test
    public void testMinimumViableSnippet() throws Exception {
        String createString = "CREATE"
                + "(mdm:TestProduct {name: \"MDM\"}),"
                + "(ftr:TestProduct {name: \"First Time Right\"}),"
                + "(suite6:TestProduct {name: \"Suite6\"}),"
                + "(dataImprover:TestProduct {name: \"Data Improver\"})";

        Connection connection = getTestDbConnection();
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(createString);

            rs = stmt.executeQuery("MATCH (n:TestProduct) RETURN n.name");
            while (rs.next()) {
                assertNotNull(rs.getString("n.name"));
            }
            
            rs = stmt.executeQuery("MATCH (n:TestProduct) DELETE n");
            assertFalse(rs.next());
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}

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

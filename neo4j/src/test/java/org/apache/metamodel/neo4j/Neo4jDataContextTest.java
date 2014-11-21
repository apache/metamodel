package org.apache.metamodel.neo4j;

import java.sql.Connection;

import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class Neo4jDataContextTest extends Neo4jTestCase {

    public void testExecuteQuery() throws Exception {
        Connection connection = getTestDbConnection();
        Neo4jDataContext strategy = new Neo4jDataContext(connection);
        Schema schema = strategy.getSchemaByName(strategy.getDefaultSchemaName());

        Query q = new Query();
        Table table = schema.getTables()[0];
//        q.from(table, "a");
//        q.select(table.getColumns());
//        assertEquals(
//                "SELECT a._CUSTOMERNUMBER_, a._CUSTOMERNAME_, a._CONTACTLASTNAME_, a._CONTACTFIRSTNAME_, a._PHONE_, "
//                        + "a._ADDRESSLINE1_, a._ADDRESSLINE2_, a._CITY_, a._STATE_, a._POSTALCODE_, a._COUNTRY_, "
//                        + "a._SALESREPEMPLOYEENUMBER_, a._CREDITLIMIT_ FROM PUBLIC._CUSTOMERS_ a", q.toString()
//                        .replace('\"', '_'));
//        DataSet result = strategy.executeQuery(q);
//        assertTrue(result.next());
//        assertEquals(
//                "Row[values=[103, Atelier graphique, Schmitt, Carine, 40.32.2555, 54, rue Royale, null, Nantes, null, "
//                        + "44000, France, 1370, 21000.0]]", result.getRow().toString());
//        assertTrue(result.next());
//        assertTrue(result.next());
//        assertTrue(result.next());
//        assertTrue(result.next());
//        assertEquals(
//                "Row[values=[121, Baane Mini Imports, Bergulfsen, Jonas, 07-98 9555, Erling Skakkes gate 78, null, "
//                        + "Stavern, null, 4110, Norway, 1504, 81700.0]]", result.getRow().toString());
//        result.close();
    }

}

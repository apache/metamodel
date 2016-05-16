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
package org.apache.metamodel.query.parser;

import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.MetaModelHelper;
import org.apache.metamodel.MockDataContext;
import org.apache.metamodel.query.FilterClause;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.OrderByItem.Direction;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;

import junit.framework.TestCase;

public class QueryParserTest extends TestCase {

    private MockDataContext dc;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        dc = new MockDataContext("sch", "tbl", "foo");

        // set 'baz' column to an integer column (to influence query generation)
        MutableColumn col = (MutableColumn) dc.getColumnByQualifiedLabel("tbl.baz");
        col.setType(ColumnType.INTEGER);
    };
	
    public void testQueryWithParenthesis() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "select foo from sch.tbl where (foo= 1) and (foo=2)");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.foo = '1' AND tbl.foo = '2'",
                q.toSql());
    }

    public void testQueryWithParenthesisAnd() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "select foo from sch.tbl where (foo= 1) and (foo=2)");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.foo = '1' AND tbl.foo = '2'", q.toSql());
    }

    public void testQueryInLowerCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "select a.foo as f from sch.tbl a inner join sch.tbl b on a.foo=b.foo order by a.foo asc");
        assertEquals("SELECT a.foo AS f FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo ORDER BY a.foo ASC",
                q.toSql());
    }

    public void testParseScalarFunctions() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "select TO_NUM(a.foo) from sch.tbl a WHERE BOOLEAN(a.bar) = false");
        assertEquals("SELECT TO_NUMBER(a.foo) FROM sch.tbl a WHERE TO_BOOLEAN(a.bar) = FALSE", q.toSql());
    }

    public void testSelectMapValueUsingDotNotation() throws Exception {
        // set 'baz' column to a MAP column
        MutableColumn col = (MutableColumn) dc.getColumnByQualifiedLabel("tbl.baz");
        col.setType(ColumnType.MAP);

        Query q = MetaModelHelper.parseQuery(dc,
                "SELECT sch.tbl.baz.foo.bar, baz.helloworld, baz.hello.world FROM sch.tbl");
        assertEquals(
                "SELECT MAP_VALUE('foo.bar',tbl.baz), MAP_VALUE('helloworld',tbl.baz), MAP_VALUE('hello.world',tbl.baz) FROM sch.tbl",
                q.toSql());
    }

    public void testSelectEverythingFromTable() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT * FROM sch.tbl");
        assertEquals("SELECT tbl.foo, tbl.bar, tbl.baz FROM sch.tbl", q.toSql());
    }

    public void testSelectEverythingFromJoin() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT * FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo");
        assertEquals(
                "SELECT a.foo, a.bar, a.baz, b.foo, b.bar, b.baz FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo",
                q.toSql());

        q = MetaModelHelper.parseQuery(dc, "SELECT a.foo, b.* FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo");
        assertEquals("SELECT a.foo, b.foo, b.bar, b.baz FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo",
                q.toSql());
    }

    public void testSelectColumnWithDotInName() throws Exception {
        MutableColumn col = (MutableColumn) dc.getTableByQualifiedLabel("tbl").getColumn(0);
        col.setName("fo.o");

        Query q = MetaModelHelper.parseQuery(dc, "SELECT fo.o AS f FROM sch.tbl");
        assertEquals("SELECT tbl.fo.o AS f FROM sch.tbl", q.toSql());
    }

    public void testApproximateCountQuery() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT APPROXIMATE COUNT(*) FROM sch.tbl");
        assertEquals("SELECT APPROXIMATE COUNT(*) FROM sch.tbl", q.toSql());
        assertTrue(q.getSelectClause().getItem(0).isFunctionApproximationAllowed());
    }

    public void testSelectAlias() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo AS f FROM sch.tbl");
        assertEquals("SELECT tbl.foo AS f FROM sch.tbl", q.toSql());

        q = MetaModelHelper.parseQuery(dc, "SELECT a.foo AS foobarbaz FROM sch.tbl a WHERE foobarbaz = '123'");
        assertEquals("SELECT a.foo AS foobarbaz FROM sch.tbl a WHERE a.foo = '123'", q.toSql());

        // assert that the referred "foobarbaz" is in fact the same select item
        // (that's not visible from the toSql() call since there
        // WhereItem.toSql() method will not use the alias)
        SelectItem selectItem1 = q.getSelectClause().getItem(0);
        SelectItem selectItem2 = q.getWhereClause().getItem(0).getSelectItem();
        assertSame(selectItem1, selectItem2);
    }

    public void testSelectDistinct() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT DISTINCT foo, bar AS f FROM sch.tbl");
        assertEquals("SELECT DISTINCT tbl.foo, tbl.bar AS f FROM sch.tbl", q.toSql());
    }

    public void testSelectDistinctInLowerCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT distinct foo, bar AS f FROM sch.tbl");
        assertEquals("SELECT DISTINCT tbl.foo, tbl.bar AS f FROM sch.tbl", q.toSql());
    }

    public void testSelectMinInLowerCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT min(tbl.foo) FROM sch.tbl");
        assertEquals("SELECT MIN(tbl.foo) FROM sch.tbl", q.toSql());
    }

    public void testSelectEmptySpacesBeforeAs() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM sch.tbl");
        assertEquals("SELECT tbl.foo AS alias FROM sch.tbl", q.toSql());
    }

    /**
     * This will test differences cases for tablename
     * 
     * @throws Exception
     */
    public void testTableName() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM sch.tbl");
        assertEquals("SELECT tbl.foo AS alias FROM sch.tbl", q.toSql());

        // Missing ] Bracket
        try {
            MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM [sch.tbl");
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals("Not capable of parsing FROM token: [sch.tbl. Expected end ]", e.getMessage());
        }

        try {
            MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM \"sch.tbl");
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals("Not capable of parsing FROM token: \"sch.tbl. Expected end \"", e.getMessage());
        }
        // Test Delimiter in tablename
        try {
            MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM \"sch.tbl");
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals("Not capable of parsing FROM token: \"sch.tbl. Expected end \"", e.getMessage());
        }

        // Positive test case
        q = MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM [sch.tbl]");
        assertEquals("SELECT tbl.foo AS alias FROM sch.tbl", q.toSql());

        q = MetaModelHelper.parseQuery(dc, "SELECT tbl.foo    AS alias FROM \"sch.tbl\"");
        assertEquals("SELECT tbl.foo AS alias FROM sch.tbl", q.toSql());

    }

    public void testSelectAvgInLowerCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT avg(tbl.foo) FROM sch.tbl");
        assertEquals("SELECT AVG(tbl.foo) FROM sch.tbl", q.toSql());
    }

    public void testSimpleSelectFrom() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo\nFROM sch.tbl");
        assertEquals("SELECT tbl.foo FROM sch.tbl", q.toSql());

        assertEquals(1, q.getFromClause().getItemCount());
        FromItem fromItem = q.getFromClause().getItem(0);
        assertNull("FROM item was an expression based item, which indicates it was not parsed",
                fromItem.getExpression());
        assertNotNull(fromItem.getTable());
        assertEquals("tbl", fromItem.getTable().getName());

        assertEquals(1, q.getSelectClause().getItemCount());
        SelectItem selectItem = q.getSelectClause().getItem(0);
        assertNull("SELECT item was an expression based item, which indicates it was not parsed",
                selectItem.getExpression());
        assertNotNull(selectItem.getColumn());
        assertEquals("foo", selectItem.getColumn().getName());

        assertNull(q.getFirstRow());
        assertNull(q.getMaxRows());
    }

    public void testCarthesianProduct() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "  SELECT a.foo,b.bar FROM      sch.tbl a, sch.tbl b \t WHERE a.foo = b.foo");
        assertEquals("SELECT a.foo, b.bar FROM sch.tbl a, sch.tbl b WHERE a.foo = b.foo", q.toSql());

        List<FromItem> fromItems = q.getFromClause().getItems();
        assertNotNull(fromItems.get(0).getTable());
        assertNotNull(fromItems.get(1).getTable());

        List<FilterItem> whereItems = q.getWhereClause().getItems();
        assertNotNull(whereItems.get(0).getSelectItem().getColumn());
        assertNotNull(whereItems.get(0).getSelectItem().getFromItem().getTable());
    }

    public void testJoin() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "SELECT a.foo,b.bar FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo");
        assertEquals("SELECT a.foo, b.bar FROM sch.tbl a INNER JOIN sch.tbl b ON a.foo = b.foo", q.toSql());

        q = MetaModelHelper.parseQuery(dc,
                "SELECT COUNT(*) FROM sch.tbl a LEFT JOIN sch.tbl b ON a.foo = b.foo AND a.bar = b.baz");
        assertEquals("SELECT COUNT(*) FROM sch.tbl a LEFT JOIN sch.tbl b ON a.foo = b.foo AND a.bar = b.baz",
                q.toSql());
    }

    public void testSimpleSelectFromWhere() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE bar = 'baz' AND baz > 5");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.bar = 'baz' AND tbl.baz > 5", q.toSql());

        FilterClause whereClause = q.getWhereClause();
        assertEquals(2, whereClause.getItemCount());
        assertNull("WHERE item was an expression based item, which indicates it was not parsed",
                whereClause.getItem(0).getExpression());
        assertEquals(2, whereClause.getItemCount());
        assertNull("WHERE item was an expression based item, which indicates it was not parsed",
                whereClause.getItem(1).getExpression());

        assertEquals("baz", whereClause.getItem(0).getOperand());
        assertEquals(Integer.class, whereClause.getItem(1).getOperand().getClass());
    }

    public void testWhereStringEscaped() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE bar = 'ba\\'z'");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.bar = 'ba'z'", q.toSql());
    }

    public void testWhereOperandIsBoolean() throws Exception {
        // set 'baz' column to an integer column (to influence query generation)
        MutableColumn col = (MutableColumn) dc.getColumnByQualifiedLabel("tbl.baz");
        col.setType(ColumnType.BOOLEAN);

        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE baz = TRUE");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.baz = TRUE", q.toSql());
    }

    public void testWhereOperandIsDate() throws Exception {
        // set 'baz' column to an integer column (to influence query generation)
        MutableColumn col = (MutableColumn) dc.getColumnByQualifiedLabel("tbl.baz");
        col.setType(ColumnType.TIME);

        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE baz = 10:24");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.baz = TIME '10:24:00'", q.toSql());
    }

    public void testCoumpoundWhereClause() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "SELECT foo FROM sch.tbl WHERE (bar = 'baz' OR (baz > 5 AND baz < 7))");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE (tbl.bar = 'baz' OR (tbl.baz > 5 AND tbl.baz < 7))", q.toSql());

        FilterClause wc = q.getWhereClause();
        assertEquals(1, wc.getItemCount());
        FilterItem item = wc.getItem(0);
        assertTrue(item.isCompoundFilter());

        FilterItem[] childItems = item.getChildItems();
        assertEquals(2, childItems.length);

        FilterItem bazConditions = childItems[1];
        assertTrue(bazConditions.isCompoundFilter());
    }

    public void testCoumpoundWhereClauseDelimInLoweCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "SELECT foo FROM sch.tbl WHERE (bar = 'baz' OR (baz > 5 and baz < 7))");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE (tbl.bar = 'baz' OR (tbl.baz > 5 AND tbl.baz < 7))", q.toSql());

        FilterClause wc = q.getWhereClause();
        assertEquals(1, wc.getItemCount());
        FilterItem item = wc.getItem(0);
        assertTrue(item.isCompoundFilter());

        FilterItem[] childItems = item.getChildItems();
        assertEquals(2, childItems.length);

        FilterItem bazConditions = childItems[1];
        assertTrue(bazConditions.isCompoundFilter());
    }

    public void testWhereSomethingIsNull() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE bar IS NULL");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.bar IS NULL", q.toSql());

        assertEquals(1, q.getWhereClause().getItemCount());
        assertNull("WHERE item was an expression based item, which indicates it was not parsed",
                q.getWhereClause().getItem(0).getExpression());
        assertNull(q.getWhereClause().getItem(0).getOperand());
        assertEquals(OperatorType.EQUALS_TO, q.getWhereClause().getItem(0).getOperator());
    }

    public void testWhereSomethingIsNotNull() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE bar IS NOT NULL");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.bar IS NOT NULL", q.toSql());

        assertEquals(1, q.getWhereClause().getItemCount());
        assertNull("WHERE item was an expression based item, which indicates it was not parsed",
                q.getWhereClause().getItem(0).getExpression());
        assertNull(q.getWhereClause().getItem(0).getOperand());
        assertEquals(OperatorType.DIFFERENT_FROM, q.getWhereClause().getItem(0).getOperator());
    }

    public void testLimitAndOffset() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl LIMIT 1234 OFFSET 5");
        assertEquals("SELECT tbl.foo FROM sch.tbl", q.toSql());
        assertEquals(1234, q.getMaxRows().intValue());
        assertEquals(6, q.getFirstRow().intValue());
    }

    public void testWhereIn() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE foo IN ('a','b',5)");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.foo IN ('a' , 'b' , '5')", q.toSql());

        FilterItem whereItem = q.getWhereClause().getItem(0);
        assertEquals(OperatorType.IN, whereItem.getOperator());
        Object operand = whereItem.getOperand();
        assertTrue(operand instanceof List);
        assertEquals("a", ((List<?>) operand).get(0));
        assertEquals("b", ((List<?>) operand).get(1));
        assertEquals(5, ((List<?>) operand).get(2));
    }

    public void testWhereInInLowerCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE foo in ('a','b',5)");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.foo IN ('a' , 'b' , '5')", q.toSql());

        FilterItem whereItem = q.getWhereClause().getItem(0);
        assertEquals(OperatorType.IN, whereItem.getOperator());
        Object operand = whereItem.getOperand();
        assertTrue(operand instanceof List);
        assertEquals("a", ((List<?>) operand).get(0));
        assertEquals("b", ((List<?>) operand).get(1));
        assertEquals(5, ((List<?>) operand).get(2));
    }

    public void testWhereLikeInLowerCase() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT foo FROM sch.tbl WHERE foo like 'a%'");
        assertEquals("SELECT tbl.foo FROM sch.tbl WHERE tbl.foo LIKE 'a%'", q.toSql());

        FilterItem whereItem = q.getWhereClause().getItem(0);
        assertEquals(OperatorType.LIKE, whereItem.getOperator());
        Object operand = whereItem.getOperand();
        assertTrue(operand instanceof String);
        assertEquals("a%", operand);
    }

    public void testSimpleSubQuery() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT f.foo AS fo FROM (SELECT * FROM sch.tbl) f");
        assertEquals("SELECT f.foo AS fo FROM (SELECT tbl.foo, tbl.bar, tbl.baz FROM sch.tbl) f", q.toSql());
    }

    public void testSelectEverythingFromSubQuery() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc, "SELECT * FROM (SELECT foo, bar FROM sch.tbl) f");
        assertEquals("SELECT f.foo, f.bar FROM (SELECT tbl.foo, tbl.bar FROM sch.tbl) f", q.toSql());
    }

    public void testGetIndicesVanillaScenario() throws Exception {
        QueryParser qp = new QueryParser(dc, "SELECT ... FROM ... BAR BAZ");
        assertEquals("[0, 7]", Arrays.toString(qp.indexesOf("SELECT ", null)));
        assertEquals("[10, 16]", Arrays.toString(qp.indexesOf(" FROM ", null)));
    }

    public void testGetIndicesIgnoreWhiteSpaceAndCaseDifferences() throws Exception {
        QueryParser qp = new QueryParser(dc, " \t\r\n select ... from ... BAR BAZ");
        assertEquals("[0, 7]", Arrays.toString(qp.indexesOf("SELECT ", null)));
        assertEquals("[10, 16]", Arrays.toString(qp.indexesOf(" FROM ", null)));
    }

    public void testInvalidQueries() throws Exception {
        try {
            MetaModelHelper.parseQuery(dc, "foobar");
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals("SELECT not found in query: foobar", e.getMessage());
        }

        try {
            MetaModelHelper.parseQuery(dc, "SELECT foobar");
            fail("Exception expected");
        } catch (MetaModelException e) {
            assertEquals("FROM not found in query: SELECT foobar", e.getMessage());
        }
    }

    public void testFullQuery() throws Exception {
        Query q = MetaModelHelper.parseQuery(dc,
                "SELECT foo, COUNT(* ), MAX( baz ) FROM sch.tbl WHERE bar = 'baz' AND foo = bar AND baz > 5 "
                        + "GROUP BY foo HAVING COUNT(*) > 2 ORDER BY foo LIMIT 20 OFFSET 10");
        assertEquals(
                "SELECT tbl.foo, COUNT(*), MAX(tbl.baz) FROM sch.tbl WHERE tbl.bar = 'baz' AND tbl.foo = tbl.bar AND tbl.baz > 5 "
                        + "GROUP BY tbl.foo HAVING COUNT(*) > 2 ORDER BY tbl.foo ASC",
                q.toSql());
        assertEquals(20, q.getMaxRows().intValue());
        assertEquals(11, q.getFirstRow().intValue());

        // SELECT ...
        // tbl.foo
        assertNotNull("SelectItem 1 should be a column", q.getSelectClause().getItem(0).getColumn());

        // COUNT(*)
        assertNotNull("SelectItem 2 should be a Function", q.getSelectClause().getItem(1).getAggregateFunction());
        assertNotNull("SelectItem 2 should be a Function of '*'", q.getSelectClause().getItem(1).getExpression());

        // MAX(tbl.baz)
        assertNotNull("SelectItem 3 should be a Function", q.getSelectClause().getItem(2).getAggregateFunction());
        assertNotNull("SelectItem 4 should be a Function of a column", q.getSelectClause().getItem(2).getColumn());

        // FROM tbl.foo
        assertNotNull(q.getFromClause().getItem(0).getTable());

        // GROUP BY tbl.foo
        assertNotNull(q.getGroupByClause().getItem(0).getSelectItem().getColumn());

        // HAVING COUNT(*) > 2
        FilterItem havingItem = q.getHavingClause().getItem(0);
        assertNull(havingItem.getExpression());
        assertNotNull(havingItem.getSelectItem().getAggregateFunction());
        assertEquals("*", havingItem.getSelectItem().getExpression());

        // ORDER BY tbl.foo ASC
        OrderByItem orderByItem = q.getOrderByClause().getItem(0);
        assertNull(orderByItem.getSelectItem().getExpression());
        assertNotNull(orderByItem.getSelectItem().getColumn());
        assertEquals(Direction.ASC, orderByItem.getDirection());
    }
}

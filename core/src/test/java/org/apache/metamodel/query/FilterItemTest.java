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
package org.apache.metamodel.query;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.CachingDataSetHeader;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class FilterItemTest extends TestCase {

    public void testExpressionBasedFilter() throws Exception {
        FilterItem filterItem = new FilterItem("foobar");
        assertEquals("foobar", filterItem.getExpression());

        try {
            filterItem.evaluate(null);
            fail("Exception should have been thrown");
        } catch (Exception e) {
            assertEquals("Expression-based filters cannot be manually evaluated", e.getMessage());
        }

        Column col1 = new MutableColumn("Col1", ColumnType.VARCHAR);
        assertEquals("SELECT Col1 WHERE foobar", new Query().select(col1).where(filterItem).toString());

        assertEquals("SELECT Col1 WHERE YEAR(Col1) = 2008", new Query().select(col1).where("YEAR(Col1) = 2008")
                .toString());
    }

    public void testToSqlWhereItem() throws Exception {
        MutableColumn col1 = new MutableColumn("Col1", ColumnType.VARCHAR);
        SelectItem selectItem = new SelectItem(col1);
        FilterItem c = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, null);
        assertEquals("Col1 IS NOT NULL", c.toString());

        try {
            c = new FilterItem(selectItem, OperatorType.GREATER_THAN, null);
            fail("Exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertEquals("Can only use EQUALS or DIFFERENT_FROM operator with null-operand", e.getMessage());
        }

        c = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "foo");
        assertEquals("Col1 <> 'foo'", c.toString());

        c = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "'bar'");

        // this will be rewritten so it's not an issue even though it look like
        // it needs an escape-char
        assertEquals("Col1 <> ''bar''", c.toSql());

        c = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "foo's bar");
        // the same applies here
        assertEquals("Col1 <> 'foo's bar'", c.toSql());

        col1.setType(ColumnType.FLOAT);
        c = new FilterItem(selectItem, OperatorType.EQUALS_TO, 423);
        assertEquals("Col1 = 423", c.toString());

        c = new FilterItem(selectItem, OperatorType.EQUALS_TO, 423426235423.42);
        assertEquals("Col1 = 423426235423.42", c.toString());

        c = new FilterItem(selectItem, OperatorType.EQUALS_TO, true);
        assertEquals("Col1 = 1", c.toString());

        c = new FilterItem(selectItem, OperatorType.GREATER_THAN_OR_EQUAL, 123);
        assertEquals("Col1 >= 123", c.toString());

        c = new FilterItem(selectItem, OperatorType.LESS_THAN_OR_EQUAL, 123);
        assertEquals("Col1 <= 123", c.toString());

        Column timeColumn = new MutableColumn("TimeCol", ColumnType.TIME);
        selectItem = new SelectItem(timeColumn);
        c = new FilterItem(selectItem, OperatorType.GREATER_THAN, "02:30:05.000");
        assertEquals("TimeCol > TIME '02:30:05'", c.toString());

        Column dateColumn = new MutableColumn("DateCol", ColumnType.DATE);
        c = new FilterItem(new SelectItem(dateColumn), OperatorType.GREATER_THAN, "2000-12-31");
        assertEquals("DateCol > DATE '2000-12-31'", c.toString());
    }

    public void testToStringTimeStamp() throws Exception {
        Column timestampColumn = new MutableColumn("TimestampCol", ColumnType.TIMESTAMP);
        FilterItem c = new FilterItem(new SelectItem(timestampColumn), OperatorType.LESS_THAN,
                "2000-12-31 02:30:05.007");
        assertEquals("TimestampCol < TIMESTAMP '2000-12-31 02:30:05'", c.toString());

        c = new FilterItem(new SelectItem(timestampColumn), OperatorType.LESS_THAN, "2000-12-31 02:30:05");
        assertEquals("TimestampCol < TIMESTAMP '2000-12-31 02:30:05'", c.toString());

        Column dateColumn = new MutableColumn("DateCol", ColumnType.DATE);
        c = new FilterItem(new SelectItem(timestampColumn), OperatorType.GREATER_THAN, new SelectItem(dateColumn));
        assertEquals("TimestampCol > DateCol", c.toString());
    }

    public void testEvaluateStrings() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.VARCHAR);
        Column col2 = new MutableColumn("Col2", ColumnType.VARCHAR);
        SelectItem s1 = new SelectItem(col1);
        SelectItem s2 = new SelectItem(col2);
        SelectItem[] selectItems = new SelectItem[] { s1, s2 };
        SimpleDataSetHeader header = new SimpleDataSetHeader(selectItems);
        Row row;
        FilterItem c;

        row = new DefaultRow(header, new Object[] { "foo", "bar" });
        c = new FilterItem(s1, OperatorType.DIFFERENT_FROM, s2);
        assertTrue(c.evaluate(row));

        row = new DefaultRow(header, new Object[] { "aaa", "bbb" });
        c = new FilterItem(s1, OperatorType.GREATER_THAN, s2);
        assertFalse(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.LESS_THAN, s2);
        assertTrue(c.evaluate(row));

        row = new DefaultRow(header, new Object[] { "aaa", "aaa" });
        c = new FilterItem(s1, OperatorType.EQUALS_TO, s2);
        assertTrue(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.LIKE, s2);
        row = new DefaultRow(header, new Object[] { "foobar", "fo%b%r" });
        assertTrue(c.evaluate(row));

        row = new DefaultRow(header, new Object[] { "foobbdbafsdfr", "fo%b%r" });
        assertTrue(c.evaluate(row));
    }

    public void testEvaluateNull() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.INTEGER);
        Column col2 = new MutableColumn("Col2", ColumnType.DECIMAL);
        SelectItem s1 = new SelectItem(col1);
        SelectItem s2 = new SelectItem(col2);
        SelectItem[] selectItems = new SelectItem[] { s1, s2 };
        CachingDataSetHeader header = new CachingDataSetHeader(selectItems);

        FilterItem c = new FilterItem(s1, OperatorType.EQUALS_TO, null);

        Row row = new DefaultRow(header, new Object[] { 1, 1 });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, 1 });
        assertTrue(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.EQUALS_TO, 1);

        row = new DefaultRow(header, new Object[] { 1, 1 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, 1 });
        assertFalse(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.DIFFERENT_FROM, 5);

        row = new DefaultRow(header, new Object[] { 1, 1 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, 1 });
        assertTrue(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.GREATER_THAN, s2);

        row = new DefaultRow(header, new Object[] { 5, 1 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, 1 });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 1, null });
        assertFalse(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.GREATER_THAN_OR_EQUAL, s2);
        row = new DefaultRow(header, new Object[] { 5, 1 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 1, 5 });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 5, 5 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, 1 });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 1, null });
        assertFalse(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.LESS_THAN_OR_EQUAL, s2);
        row = new DefaultRow(header, new Object[] { 1, 5 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 5, 1 });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 1, 1 });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, 1 });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { 1, null });
        assertFalse(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.EQUALS_TO, s2);
        row = new DefaultRow(header, new Object[] { 1, null });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { null, null });
        assertTrue(c.evaluate(row));
    }

    public void testEvaluateDates() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.DATE);
        SelectItem s1 = new SelectItem(col1);
        SelectItem[] selectItems = new SelectItem[] { s1 };
        CachingDataSetHeader header = new CachingDataSetHeader(selectItems);

        long currentTimeMillis = System.currentTimeMillis();
        FilterItem c = new FilterItem(s1, OperatorType.LESS_THAN, new java.sql.Date(currentTimeMillis));

        Row row = new DefaultRow(header, new Object[] { new java.sql.Date(currentTimeMillis) });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { new java.sql.Date(currentTimeMillis + 10000000) });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { new java.sql.Date(currentTimeMillis - 10000000) });
        assertTrue(c.evaluate(row));
    }

    public void testEvaluateBooleans() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.BIT);
        SelectItem s1 = new SelectItem(col1);
        SelectItem[] selectItems = new SelectItem[] { s1 };
        DataSetHeader header = new SimpleDataSetHeader(selectItems);

        FilterItem c = new FilterItem(s1, OperatorType.EQUALS_TO, true);

        Row row = new DefaultRow(header, new Object[] { true });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { false });
        assertFalse(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.EQUALS_TO, false);
        row = new DefaultRow(header, new Object[] { true });
        assertFalse(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { false });
        assertTrue(c.evaluate(row));

        c = new FilterItem(s1, OperatorType.GREATER_THAN, false);
        row = new DefaultRow(header, new Object[] { true });
        assertTrue(c.evaluate(row));
        row = new DefaultRow(header, new Object[] { false });
        assertFalse(c.evaluate(row));
    }

    /**
     * Tests that the following (general) rules apply to the object:
     * <p/>
     * <li>the hashcode is the same when run twice on an unaltered object</li>
     * <li>if o1.equals(o2) then this condition must be true: o1.hashCode() ==
     * 02.hashCode()
     */
    public void testEqualsAndHashCode() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.BIT);

        FilterItem c1 = new FilterItem(new SelectItem(col1), OperatorType.EQUALS_TO, true);
        FilterItem c2 = new FilterItem(new SelectItem(col1), OperatorType.EQUALS_TO, true);
        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());

        c2 = new FilterItem(new SelectItem(col1), OperatorType.GREATER_THAN, true);
        assertFalse(c1.equals(c2));
        assertFalse(c1.hashCode() == c2.hashCode());

        Column col2 = new MutableColumn("Col2", ColumnType.VARBINARY);
        c2 = new FilterItem(new SelectItem(col2), OperatorType.EQUALS_TO, true);
        assertFalse(c1.equals(c2));
        assertFalse(c1.hashCode() == c2.hashCode());
    }

    public void testOrFilterItem() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.VARCHAR);

        SelectItem s1 = new SelectItem(col1);
        FilterItem c1 = new FilterItem(s1, OperatorType.EQUALS_TO, "foo");
        FilterItem c2 = new FilterItem(s1, OperatorType.EQUALS_TO, "bar");
        FilterItem c3 = new FilterItem(s1, OperatorType.EQUALS_TO, "foobar");

        FilterItem filter = new FilterItem(c1, c2, c3);
        assertEquals("(Col1 = 'foo' OR Col1 = 'bar' OR Col1 = 'foobar')", filter.toString());

        DataSetHeader header = new SimpleDataSetHeader(new SelectItem[] { s1 });

        assertTrue(filter.evaluate(new DefaultRow(header, new Object[] { "foo" })));
        assertTrue(filter.evaluate(new DefaultRow(header, new Object[] { "bar" })));
        assertTrue(filter.evaluate(new DefaultRow(header, new Object[] { "foobar" })));

        assertFalse(filter.evaluate(new DefaultRow(header, new Object[] { "foob" })));
    }

    public void testAndFilterItem() throws Exception {
        Column col1 = new MutableColumn("Col1", ColumnType.VARCHAR);

        SelectItem s1 = new SelectItem(col1);
        FilterItem c1 = new FilterItem(s1, OperatorType.LIKE, "foo%");
        FilterItem c2 = new FilterItem(s1, OperatorType.LIKE, "%bar");
        FilterItem c3 = new FilterItem(s1, OperatorType.DIFFERENT_FROM, "foobar");

        FilterItem filter = new FilterItem(LogicalOperator.AND, c1, c2, c3);
        assertEquals("(Col1 LIKE 'foo%' AND Col1 LIKE '%bar' AND Col1 <> 'foobar')", filter.toString());

        SelectItem[] items = new SelectItem[] { s1 };
        CachingDataSetHeader header = new CachingDataSetHeader(items);
        assertTrue(filter.evaluate(new DefaultRow(header, new Object[] { "foo bar" })));
        assertTrue(filter.evaluate(new DefaultRow(header, new Object[] { "foosenbar" })));
        assertFalse(filter.evaluate(new DefaultRow(header, new Object[] { "foo" })));
        assertFalse(filter.evaluate(new DefaultRow(header, new Object[] { "hello world" })));
        assertFalse(filter.evaluate(new DefaultRow(header, new Object[] { "foobar" })));
    }

    // Ticket #410
    public void testOrFilterItemWithoutSelectingActualItmes() throws Exception {

        // define the schema
        final MutableSchema schema = new MutableSchema("s");
        MutableTable table = new MutableTable("persons", TableType.TABLE, schema);
        schema.addTable(table);
        final Column col1 = new MutableColumn("name", ColumnType.VARCHAR, table, 1, true);
        final Column col2 = new MutableColumn("role", ColumnType.VARCHAR, table, 2, true);
        final Column col3 = new MutableColumn("column_number", ColumnType.INTEGER, table, 3, true);
        table.addColumn(col1);
        table.addColumn(col2);
        table.addColumn(col3);

        Query q = new Query();
        q.select(col3);
        q.from(col1.getTable());

        SelectItem selectItem1 = new SelectItem(col1);
        SelectItem selectItem2 = new SelectItem(col2);

        FilterItem item1 = new FilterItem(selectItem1, OperatorType.EQUALS_TO, "kasper");
        FilterItem item2 = new FilterItem(selectItem2, OperatorType.EQUALS_TO, "user");

        q.where(new FilterItem(item1, item2));

        assertEquals(
                "SELECT persons.column_number FROM s.persons WHERE (persons.name = 'kasper' OR persons.role = 'user')",
                q.toString());

        DataContext dc = new QueryPostprocessDataContext() {

            @Override
            public DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
                // we expect 3 columns to be materialized because the query has column references in both SELECT and WHERE clause
                assertEquals(3, columns.length);
                assertEquals("column_number", columns[0].getName());
                assertEquals("name", columns[1].getName());
                assertEquals("role", columns[2].getName());
                SelectItem[] selectItems = new SelectItem[] { new SelectItem(col1), new SelectItem(col2),
                        new SelectItem(col3) };
                DataSetHeader header = new CachingDataSetHeader(selectItems);
                List<Row> rows = new LinkedList<Row>();
                rows.add(new DefaultRow(header, new Object[] { "foo", "bar", 1 }));
                rows.add(new DefaultRow(header, new Object[] { "kasper", "developer", 2 }));
                rows.add(new DefaultRow(header, new Object[] { "admin", "admin", 3 }));
                rows.add(new DefaultRow(header, new Object[] { "elikeon", "user", 4 }));
                rows.add(new DefaultRow(header, new Object[] { "someuser", "user", 5 }));
                rows.add(new DefaultRow(header, new Object[] { "hmm", "what-the", 6 }));

                return new InMemoryDataSet(header, rows);
            }

            @Override
            protected String getMainSchemaName() throws MetaModelException {
                return "s";
            }

            @Override
            protected Schema getMainSchema() throws MetaModelException {
                return schema;
            }
        };

        DataSet result = dc.executeQuery(q);
        List<Object[]> objectArrays = result.toObjectArrays();
        assertEquals(3, objectArrays.size());
        assertEquals(2, objectArrays.get(0)[0]);
        assertEquals(4, objectArrays.get(1)[0]);
        assertEquals(5, objectArrays.get(2)[0]);
    }

    public void testInOperandSql() throws Exception {
        SelectItem selectItem = new SelectItem(new MutableColumn("foo", ColumnType.VARCHAR, null, 1, null, null, true,
                null, false, null));
        Object operand = new String[] { "foo", "bar" };
        assertEquals("foo IN ('foo' , 'bar')", new FilterItem(selectItem, OperatorType.IN, operand).toSql());

        operand = Arrays.asList("foo", "bar", "baz");
        assertEquals("foo IN ('foo' , 'bar' , 'baz')", new FilterItem(selectItem, OperatorType.IN, operand).toSql());

        operand = "foo";
        assertEquals("foo IN ('foo')", new FilterItem(selectItem, OperatorType.IN, operand).toSql());

        operand = new ArrayList<Object>();
        assertEquals("foo IN ()", new FilterItem(selectItem, OperatorType.IN, operand).toSql());
    }

    public void testNotInOperandSql() throws Exception {
        SelectItem selectItem = new SelectItem("foo", "foo");
        Object operand = new String[] { "foo", "bar" };
        assertEquals("foo NOT IN ('foo' , 'bar')", new FilterItem(selectItem, OperatorType.NOT_IN, operand).toSql());

        operand = Arrays.asList("foo", "bar", "baz");
        assertEquals("foo NOT IN ('foo' , 'bar' , 'baz')", new FilterItem(selectItem, OperatorType.NOT_IN, operand).toSql());

        operand = "foo";
        assertEquals("foo NOT IN ('foo')", new FilterItem(selectItem, OperatorType.NOT_IN, operand).toSql());

        operand = new ArrayList<Object>();
        assertEquals("foo NOT IN ()", new FilterItem(selectItem, OperatorType.NOT_IN, operand).toSql());
    }

    public void testNotLikeOperandSql() throws Exception {
        Column column = new MutableColumn("foo");
        SelectItem selectItem = new SelectItem(column);
        String operand = "%foo";
        assertEquals("foo NOT LIKE '%foo'", new FilterItem(selectItem, OperatorType.NOT_LIKE, operand).toSql());

        operand = "foo%";
        assertEquals("foo NOT LIKE 'foo%'", new FilterItem(selectItem, OperatorType.NOT_LIKE, operand).toSql());

        operand = "%foo%foo%";
        assertEquals("foo NOT LIKE '%foo%foo%'", new FilterItem(selectItem, OperatorType.NOT_LIKE, operand).toSql());
    }

    public void testInOperandEvaluate() throws Exception {
        SelectItem selectItem = new SelectItem(new MutableColumn("foo", ColumnType.VARCHAR, null, 1, null, null, true,
                null, false, null));
        Object operand = new String[] { "foo", "bar" };

        FilterItem filterItem = new FilterItem(selectItem, OperatorType.IN, operand);
        SelectItem[] selectItems = new SelectItem[] { selectItem };
        DataSetHeader header = new CachingDataSetHeader(selectItems);

        assertTrue(filterItem.evaluate(new DefaultRow(header, new Object[] { "foo" })));
        assertTrue(filterItem.evaluate(new DefaultRow(header, new Object[] { "bar" })));
        assertFalse(filterItem.evaluate(new DefaultRow(header, new Object[] { "foobar" })));
    }
}
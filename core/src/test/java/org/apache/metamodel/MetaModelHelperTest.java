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
package org.apache.metamodel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.EmptyDataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.data.SubSelectionDataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.FromItem;
import org.apache.metamodel.query.JoinType;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.OrderByItem;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class MetaModelHelperTest extends MetaModelTestCase {

    public void testLeftJoin() throws Exception {
        SelectItem si1 = new SelectItem(new MutableColumn("person_id", ColumnType.INTEGER));
        SelectItem si2 = new SelectItem(new MutableColumn("person_name", ColumnType.VARCHAR));
        SelectItem si3 = new SelectItem(new MutableColumn("person_age", ColumnType.INTEGER));
        SelectItem si4 = new SelectItem(new MutableColumn("person_role_id", ColumnType.INTEGER));
        SelectItem si5 = new SelectItem(new MutableColumn("role_id", ColumnType.INTEGER));
        SelectItem si6 = new SelectItem(new MutableColumn("role_name", ColumnType.VARCHAR));
        SelectItem si7 = new SelectItem(new MutableColumn("role_code", ColumnType.VARCHAR));
        List<Object[]> data1 = new ArrayList<Object[]>();
        data1.add(new Object[] { 1, "peter", 18, 1 });
        data1.add(new Object[] { 2, "tom", 19, 2 });
        data1.add(new Object[] { 3, "betty", 19, null });
        data1.add(new Object[] { 4, "barbara", 17, 3 });
        data1.add(new Object[] { 5, "susie", 18, 4 });

        List<Object[]> data2 = new ArrayList<Object[]>();
        data2.add(new Object[] { 1, "class president", "clpr" });
        data2.add(new Object[] { 2, "bad boy", "bb" });
        data2.add(new Object[] { 4, "trying harder", "try" });

        DataSet ds1 = createDataSet(new SelectItem[] { si1, si2, si3, si4 }, data1);
        DataSet ds2 = createDataSet(new SelectItem[] { si5, si6, si7 }, data2);
        FilterItem[] onConditions = new FilterItem[1];
        onConditions[0] = new FilterItem(si4, OperatorType.EQUALS_TO, si5);

        DataSet result = MetaModelHelper.getLeftJoin(ds1, ds2, onConditions);
        List<Object[]> objectArrays = result.toObjectArrays();
        assertEquals("[1, peter, 18, 1, 1, class president, clpr]", Arrays.toString(objectArrays.get(0)));
        assertEquals("[2, tom, 19, 2, 2, bad boy, bb]", Arrays.toString(objectArrays.get(1)));
        assertEquals("[3, betty, 19, null, null, null, null]", Arrays.toString(objectArrays.get(2)));
        assertEquals("[4, barbara, 17, 3, null, null, null]", Arrays.toString(objectArrays.get(3)));
        assertEquals("[5, susie, 18, 4, 4, trying harder, try]", Arrays.toString(objectArrays.get(4)));
        assertEquals(5, objectArrays.size());
    }

    public void testRightJoin() throws Exception {
        SelectItem si1 = new SelectItem(new MutableColumn("person_id", ColumnType.INTEGER));
        SelectItem si2 = new SelectItem(new MutableColumn("person_name", ColumnType.VARCHAR));
        SelectItem si3 = new SelectItem(new MutableColumn("person_age", ColumnType.INTEGER));
        SelectItem si4 = new SelectItem(new MutableColumn("person_role_id", ColumnType.INTEGER));
        SelectItem si5 = new SelectItem(new MutableColumn("role_id", ColumnType.INTEGER));
        SelectItem si6 = new SelectItem(new MutableColumn("role_name", ColumnType.VARCHAR));
        SelectItem si7 = new SelectItem(new MutableColumn("role_code", ColumnType.VARCHAR));
        List<Object[]> data1 = new ArrayList<Object[]>();
        data1.add(new Object[] { 1, "peter", 18, 1 });
        data1.add(new Object[] { 2, "tom", 19, 2 });
        data1.add(new Object[] { 3, "betty", 19, null });
        data1.add(new Object[] { 4, "barbara", 17, 3 });

        List<Object[]> data2 = new ArrayList<Object[]>();
        data2.add(new Object[] { 1, "class president", "clpr" });
        data2.add(new Object[] { 2, "bad boy", "bb" });
        data2.add(new Object[] { 4, "trying harder", "try" });

        DataSet ds1 = createDataSet(new SelectItem[] { si1, si2, si3, si4 }, data1);
        DataSet ds2 = createDataSet(new SelectItem[] { si5, si6, si7 }, data2);
        FilterItem[] onConditions = new FilterItem[1];
        onConditions[0] = new FilterItem(si4, OperatorType.EQUALS_TO, si5);

        DataSet result = MetaModelHelper.getRightJoin(ds1, ds2, onConditions);
        List<Object[]> objectArrays = result.toObjectArrays();
        assertEquals("[1, peter, 18, 1, 1, class president, clpr]", Arrays.toString(objectArrays.get(0)));
        assertEquals("[2, tom, 19, 2, 2, bad boy, bb]", Arrays.toString(objectArrays.get(1)));
        assertEquals("[null, null, null, null, 4, trying harder, try]", Arrays.toString(objectArrays.get(2)));
        assertEquals(3, objectArrays.size());
    }

    public void testSimpleCarthesianProduct() throws Exception {
        DataSet dataSet = MetaModelHelper.getCarthesianProduct(createDataSet1(), createDataSet2());
        List<String> results = new ArrayList<String>();

        while (dataSet.next()) {
            results.add(dataSet.getRow().toString());
        }
        assertEquals(2, dataSet.getSelectItems().length);
        assertEquals(9, results.size());
        assertTrue(results.contains("Row[values=[f, b]]"));
        assertTrue(results.contains("Row[values=[f, a]]"));
        assertTrue(results.contains("Row[values=[f, r]]"));
        assertTrue(results.contains("Row[values=[o, b]]"));
        assertTrue(results.contains("Row[values=[o, a]]"));
        assertTrue(results.contains("Row[values=[o, r]]"));
    }

    public void testTripleCarthesianProduct() throws Exception {
        DataSet dataSet = MetaModelHelper.getCarthesianProduct(createDataSet1(), createDataSet2(), createDataSet3());
        assertEquals(4, dataSet.getSelectItems().length);
        for (int i = 0; i < 3 * 3 * 2; i++) {
            assertTrue("Assertion failed at i=" + i, dataSet.next());
        }
        assertFalse(dataSet.next());
    }

    public void testTripleCarthesianProductWithWhereItems() throws Exception {
        DataSet ds1 = createDataSet1();
        DataSet ds2 = createDataSet2();
        DataSet[] dataSets = new DataSet[] { ds1, ds2, };
        FilterItem w1 = new FilterItem(ds1.getSelectItems()[0], OperatorType.EQUALS_TO, "f");
        DataSet dataSet = MetaModelHelper.getCarthesianProduct(dataSets, w1);
        assertEquals(2, dataSet.getSelectItems().length);
        for (int i = 0; i < 1 * 3; i++) {
            assertTrue("Assertion failed at i=" + i, dataSet.next());
            assertEquals("f", dataSet.getRow().getValue(0));
        }
        assertFalse(dataSet.next());
    }

    public void testGetCarthesianProductNoRows() throws Exception {
        DataSet dataSet = MetaModelHelper.getCarthesianProduct(createDataSet4(), createDataSet2(), createDataSet3());
        assertEquals(4, dataSet.getSelectItems().length);
        assertFalse(dataSet.next());

        dataSet = MetaModelHelper.getCarthesianProduct(createDataSet1(), createDataSet4(), createDataSet3());
        assertEquals(4, dataSet.getSelectItems().length);
        assertFalse(dataSet.next());

        dataSet = MetaModelHelper.getCarthesianProduct(createDataSet1(), createDataSet2(), createDataSet4());
        assertEquals(3, dataSet.getSelectItems().length);
        assertFalse(dataSet.next());
    }

    public void testGetOrdered() throws Exception {
        DataSet dataSet = createDataSet3();
        List<OrderByItem> orderByItems = new ArrayList<OrderByItem>();
        orderByItems.add(new OrderByItem(dataSet.getSelectItems()[0]));

        dataSet = MetaModelHelper.getOrdered(dataSet, orderByItems);
        assertTrue(dataSet.next());
        assertEquals("Row[values=[w00p, true]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[yippie, false]]", dataSet.getRow().toString());
        assertFalse(dataSet.next());
    }

    private DataSet createDataSet1() {
        List<Object[]> data1 = new ArrayList<Object[]>();
        data1.add(new Object[] { "f" });
        data1.add(new Object[] { "o" });
        data1.add(new Object[] { "o" });
        DataSet dataSet1 = createDataSet(new SelectItem[] { new SelectItem(new MutableColumn("foo",
                ColumnType.VARCHAR)) }, data1);
        return dataSet1;
    }

    private DataSet createDataSet2() {
        List<Object[]> data2 = new ArrayList<Object[]>();
        data2.add(new Object[] { "b" });
        data2.add(new Object[] { "a" });
        data2.add(new Object[] { "r" });
        DataSet dataSet2 = createDataSet(new SelectItem[] { new SelectItem("bar", "bar") }, data2);
        return dataSet2;
    }

    private DataSet createDataSet3() {
        List<Object[]> data3 = new ArrayList<Object[]>();
        data3.add(new Object[] { "w00p", true });
        data3.add(new Object[] { "yippie", false });
        DataSet dataSet3 = createDataSet(new SelectItem[] { new SelectItem("expression", "e"), new SelectItem("webish?",
                "w") }, data3);
        return dataSet3;
    }

    private DataSet createDataSet4() {
        List<Object[]> data4 = new ArrayList<Object[]>();
        DataSet dataSet4 = createDataSet(new SelectItem[] { new SelectItem("abc", "abc") }, data4);
        return dataSet4;
    }

    private int bigDataSetSize = 3000;

    /**
     * 
     * @return a big dataset, mocking an employee table
     */
    private DataSet createDataSet5() {
        List<Object[]> data5 = new ArrayList<Object[]>();

        for (int i = 0; i < bigDataSetSize; i++) {
            data5.add(new Object[] { i, "Person_" + i, bigDataSetSize - (i + 1) });
        }

        DataSet dataSet5 = createDataSet(new SelectItem[] { new SelectItem(new MutableColumn("nr", ColumnType.BIGINT)),
                new SelectItem(new MutableColumn("name", ColumnType.STRING)), new SelectItem(new MutableColumn("dnr",
                        ColumnType.BIGINT)) }, data5);
        return dataSet5;
    }

    /**
     * 
     * @return a big dataset, mocking an department table
     */
    private DataSet createDataSet6() {
        List<Object[]> data6 = new ArrayList<Object[]>();

        for (int i = 0; i < bigDataSetSize; i++) {
            data6.add(new Object[] { i, "Department_" + i });
        }

        DataSet dataSet6 = createDataSet(new SelectItem[] { new SelectItem(new MutableColumn("nr", ColumnType.BIGINT)),
                new SelectItem(new MutableColumn("name", ColumnType.STRING)), }, data6);
        return dataSet6;
    }

    public void testGetTables() throws Exception {
        MutableTable table1 = new MutableTable("table1");
        MutableTable table2 = new MutableTable("table2");
        MutableColumn t1column1 = new MutableColumn("t1c1", ColumnType.BIGINT);
        MutableColumn t2column1 = new MutableColumn("t2c1", ColumnType.BIGINT);
        MutableColumn t2column2 = new MutableColumn("t2c2", ColumnType.BIGINT);
        table1.addColumn(t1column1);
        t1column1.setTable(table1);
        table2.addColumn(t2column1);
        t2column1.setTable(table2);
        table2.addColumn(t2column2);
        t2column2.setTable(table2);

        ArrayList<Table> tableList = new ArrayList<Table>();
        tableList.add(table1);

        ArrayList<Column> columnList = new ArrayList<Column>();
        columnList.add(t2column1);

        Table[] tables = MetaModelHelper.getTables(tableList, columnList);
        assertEquals(2, tables.length);
        assertTrue(Arrays.asList(tables).contains(table1));
        assertTrue(Arrays.asList(tables).contains(table2));
    }

    public void testGetTableColumns() throws Exception {
        MutableTable table1 = new MutableTable("table1");
        MutableColumn column1 = new MutableColumn("c1", ColumnType.BIGINT);
        MutableColumn column2 = new MutableColumn("c2", ColumnType.BIGINT);
        MutableColumn column3 = new MutableColumn("c3", ColumnType.BIGINT);
        table1.addColumn(column1);
        column1.setTable(table1);
        table1.addColumn(column2);
        column2.setTable(table1);
        table1.addColumn(column3);
        column3.setTable(table1);

        ArrayList<Column> columnList = new ArrayList<Column>();

        Column[] columns = MetaModelHelper.getTableColumns(table1, columnList);
        assertEquals(0, columns.length);

        columnList.add(column1);
        columnList.add(column3);

        columns = MetaModelHelper.getTableColumns(table1, columnList);
        assertEquals(2, columns.length);
        assertSame(column1, columns[0]);
        assertSame(column3, columns[1]);
    }

    public void testGetTableFromItems() throws Exception {
        Schema schema = getExampleSchema();
        Table contributorTable = schema.getTableByName(TABLE_CONTRIBUTOR);
        Table projectTable = schema.getTableByName(TABLE_PROJECT);
        Table projectContributorTable = schema.getTableByName(TABLE_PROJECT_CONTRIBUTOR);

        FromItem sqFromItem = new FromItem(new Query().from(projectTable).from(projectContributorTable));
        FromItem fromItem = new FromItem(JoinType.INNER, new FromItem(contributorTable), sqFromItem, new SelectItem[0],
                new SelectItem[0]);
        Query q = new Query().from(fromItem);

        FromItem[] fromItems = MetaModelHelper.getTableFromItems(q);
        assertEquals(3, fromItems.length);
        assertEquals("[MetaModelSchema.contributor, MetaModelSchema.project, MetaModelSchema.project_contributor]",
                Arrays.toString(fromItems));
    }

    public void testGetSelectionNoRows() throws Exception {
        SelectItem item1 = new SelectItem("foo", "f");
        SelectItem item2 = new SelectItem("bar", "b");
        SelectItem item3 = new SelectItem("baz", "bz");
        List<SelectItem> selectItems1 = Arrays.asList(item1, item2, item3);
        List<SelectItem> selectItems2 = Arrays.asList(item2, item1);

        DataSet ds = MetaModelHelper.getSelection(selectItems2, new EmptyDataSet(selectItems1));
        assertEquals(SubSelectionDataSet.class, ds.getClass());

        assertEquals("[bar AS b, foo AS f]", Arrays.toString(ds.getSelectItems()));
    }

    public void testLeftJoinNoRowsOrSingleRow() throws Exception {
        SelectItem item1 = new SelectItem("foo", "f");
        SelectItem item2 = new SelectItem("bar", "b");
        SelectItem item3 = new SelectItem("baz", "z");
        List<SelectItem> selectItems1 = Arrays.asList(item1, item2);
        List<SelectItem> selectItems2 = Arrays.asList(item3);

        DataSet ds1 = new EmptyDataSet(selectItems1);
        DataSet ds2 = new EmptyDataSet(selectItems2);

        DataSet joinedDs = MetaModelHelper.getLeftJoin(ds1, ds2, new FilterItem[] { new FilterItem(item2,
                OperatorType.EQUALS_TO, item3) });

        assertEquals(SubSelectionDataSet.class, joinedDs.getClass());
        assertEquals("[foo AS f, bar AS b, baz AS z]", Arrays.toString(joinedDs.getSelectItems()));

        DataSetHeader header1 = new SimpleDataSetHeader(selectItems1);
        Row row = new DefaultRow(header1, new Object[] { 1, 2 }, null);
        ds1 = new InMemoryDataSet(header1, row);

        joinedDs = MetaModelHelper.getLeftJoin(ds1, ds2, new FilterItem[] { new FilterItem(item2,
                OperatorType.EQUALS_TO, item3) });
        assertEquals("[foo AS f, bar AS b, baz AS z]", Arrays.toString(joinedDs.getSelectItems()));
        assertTrue(joinedDs.next());
        assertEquals("Row[values=[1, 2, null]]", joinedDs.getRow().toString());
        assertFalse(joinedDs.next());
    }

    public void testCarthesianProductScalability() {

        DataSet employees = createDataSet5();
        DataSet departmens = createDataSet6();

        FilterItem fi = new FilterItem(employees.getSelectItems()[2], OperatorType.EQUALS_TO, departmens
                .getSelectItems()[0]);

        DataSet joined = MetaModelHelper.getCarthesianProduct(new DataSet[] { employees, departmens }, fi);
        int count = 0;
        while (joined.next()) {
            count++;
        }

        assertTrue(count == bigDataSetSize);

    }
}

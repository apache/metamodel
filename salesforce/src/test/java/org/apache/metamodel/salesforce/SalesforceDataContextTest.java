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
package org.apache.metamodel.salesforce;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class SalesforceDataContextTest extends SalesforceTestCase {

    public void testQueryStrangeRecord() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        SalesforceDataContext dc = new SalesforceDataContext(getUsername(), getPassword(), getSecurityToken());

        Column[] timeColumns = dc.getDefaultSchema().getTableByName("Contact").getTimeBasedColumns();
        assertEquals(
                "[Column[name=Birthdate,columnNumber=30,type=DATE,nullable=true,nativeType=date,columnSize=0], "
                        + "Column[name=CreatedDate,columnNumber=33,type=DATE,nullable=false,nativeType=datetime,columnSize=0], "
                        + "Column[name=LastModifiedDate,columnNumber=35,type=DATE,nullable=false,nativeType=datetime,columnSize=0], "
                        + "Column[name=SystemModstamp,columnNumber=37,type=DATE,nullable=false,nativeType=datetime,columnSize=0], "
                        + "Column[name=LastActivityDate,columnNumber=38,type=DATE,nullable=true,nativeType=date,columnSize=0], "
                        + "Column[name=LastCURequestDate,columnNumber=39,type=DATE,nullable=true,nativeType=datetime,columnSize=0], "
                        + "Column[name=LastCUUpdateDate,columnNumber=40,type=DATE,nullable=true,nativeType=datetime,columnSize=0], "
                        + "Column[name=EmailBouncedDate,columnNumber=42,type=DATE,nullable=true,nativeType=datetime,columnSize=0]]",
                Arrays.toString(timeColumns));
        DataSet ds = dc.query().from("Contact").select("LastModifiedDate").where("Id").eq("003b0000006xfAUAAY")
                .execute();
        if (ds.next()) {
            System.out.println(ds.getRow());
            assertFalse(ds.next());
        }
        ds.close();
    }

    public void testInvalidLoginException() throws Exception {
        try {
            new SalesforceDataContext("foo", "bar", "baz");
            fail("Exception expected");
        } catch (IllegalStateException e) {
            assertEquals(
                    "Failed to log in to Salesforce service: INVALID_LOGIN: Invalid username, password, security token; or user locked out.",
                    e.getMessage());
        }
    }

    public void testGetSchema() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        SalesforceDataContext dc = new SalesforceDataContext(getUsername(), getPassword(), getSecurityToken());

        Schema schema = dc.getDefaultSchema();

        assertEquals("Salesforce", schema.getName());

        String[] tableNames = schema.getTableNames();

        System.out.println("All tables:\n" + Arrays.toString(tableNames));

        Table accountTable = schema.getTableByName("Account");
        assertNotNull(accountTable);

        String[] columnNames = accountTable.getColumnNames();
        System.out.println("Account table columns: " + Arrays.toString(columnNames));

        Column idColumn = accountTable.getColumnByName("Id");
        Column nameColumn = accountTable.getColumnByName("Name");

        assertNotNull(idColumn);
        assertNotNull(nameColumn);

        assertEquals("Column[name=Id,columnNumber=0,type=VARCHAR,nullable=false,nativeType=id,columnSize=18]",
                idColumn.toString());
        assertEquals("id", idColumn.getNativeType());
        assertTrue(idColumn.isPrimaryKey());

        assertEquals("Column[name=Name,columnNumber=3,type=VARCHAR,nullable=false,nativeType=string,columnSize=255]",
                nameColumn.toString());
        assertEquals("string", nameColumn.getNativeType());
        assertFalse(nameColumn.isPrimaryKey());
    }

    public void testConversionOfTypes() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        SalesforceDataContext dc = new SalesforceDataContext(getUsername(), getPassword(), getSecurityToken());

        runConversionTest(dc, "Account");
        runConversionTest(dc, "Contact");
    }

    private void runConversionTest(SalesforceDataContext dc, String tableName) {
        Query q = dc.query().from(tableName).selectAll().toQuery();
        q.setMaxRows(1);

        final DataSet ds = dc.executeQuery(q);
        final SelectItem[] selectItems = ds.getSelectItems();
        while (ds.next()) {
            Row row = ds.getRow();

            for (SelectItem selectItem : selectItems) {
                Column column = selectItem.getColumn();
                Object value = row.getValue(column);
                if (value != null) {
                    ColumnType type = column.getType();
                    Class<?> expected = type.getJavaEquivalentClass();
                    Class<? extends Object> actual = value.getClass();
                    assertEquals("Unexpected type of value: " + value + ". Expected " + expected.getName()
                            + " but found " + actual.getName() + ". Native type was: " + column.getNativeType(),
                            expected, actual);
                }
            }
        }
    }

    public void testQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        SalesforceDataContext dc = new SalesforceDataContext(getUsername(), getPassword(), getSecurityToken());

        DataSet ds;

        // a very simple query
        ds = dc.query().from("Account").select("Name").execute();
        assertTrue(ds instanceof SalesforceDataSet);
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertTrue(ds.next());
        ds.close();

        // a very simple query
        Query query = dc.query().from("Account").select("Id").and("Name").where("Name").like("% %").orderBy("Name")
                .toQuery();
        query.setMaxRows(10);
        ds = dc.executeQuery(query);
        assertTrue(ds instanceof SalesforceDataSet);
        assertTrue(ds.next());
        ds.close();

        // a COUNT() query
        ds = dc.query().from("Account").selectCount().execute();
        assertFalse(ds instanceof SalesforceDataSet);
        assertTrue(ds.next());
        assertTrue(ds.getRow().getValue(0) instanceof Number);
        assertTrue(((Number) ds.getRow().getValue(0)).intValue() > 0);
        assertFalse(ds.next());
        ds.close();
    }

    public void testInsertUpdateAndDelete() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        SalesforceDataContext dc = new SalesforceDataContext(getUsername(), getPassword(), getSecurityToken());

        final String tableName = "Account";
        final String insertedName = "MetaModel TESTER contact";

        // INSERT

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.insertInto(tableName).value("name", insertedName).execute();
            }
        });

        final List<String> ids = new ArrayList<String>();

        DataSet ds;
        ds = dc.query().from(tableName).select("id", "name").where("name").eq(insertedName).execute();
        assertTrue(ds.next());
        Row row = ds.getRow();
        assertNotNull(row.getValue(0));
        ids.add(row.getValue(0).toString());

        while (ds.next()) {
            row = ds.getRow();
            ids.add(row.getValue(0).toString());
            assertEquals("MetaModel TESTER contact", row.getValue(1));
        }

        ds.close();

        // UPDATE

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.update(tableName).where("id").eq(ids.get(0)).value("name", "Another test value").execute();
            }
        });

        ds = dc.query().from(tableName).select("name").where("id").eq(ids.get(0)).execute();
        assertTrue(ds.next());
        assertEquals("Another test value", ds.getRow().getValue(0));
        assertFalse(ds.next());
        ds.close();

        // UPDATE (a record that does not exist)

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.update(tableName).where("id").eq("fooooooobaaaaaaaar")
                        .value("name", "A test value that should never occur").execute();
            }
        });

        // DELETE

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.deleteFrom(tableName).where("id").in(ids).execute();
            }
        });

        ds = dc.query().from(tableName).selectCount().where("name").eq(insertedName).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[0]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();
    }

    public void testRewriteWhereItem() throws Exception {
        final StringBuilder sb = new StringBuilder("FOOBAR: ");

        final Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        cal.setTimeInMillis(0);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.YEAR, 2013);
        cal.set(Calendar.MONTH, Calendar.JANUARY);
        cal.set(Calendar.DAY_OF_MONTH, 23);
        final Date date = cal.getTime();
        final Timestamp dateTime = new Timestamp(date.getTime());

        final List<FilterItem> children = new ArrayList<FilterItem>();
        children.add(new FilterItem(new SelectItem(new MutableColumn("foo")), OperatorType.EQUALS_TO, "hello\n 'world'"));
        children.add(new FilterItem(new SelectItem(new MutableColumn("bar")), OperatorType.EQUALS_TO, 123));
        children.add(new FilterItem(new SelectItem(new MutableColumn("baz").setType(ColumnType.DATE)),
                OperatorType.EQUALS_TO, date));
        children.add(new FilterItem(new SelectItem(new MutableColumn("zaz").setType(ColumnType.TIMESTAMP)),
                OperatorType.EQUALS_TO, date));
        children.add(new FilterItem(new SelectItem(new MutableColumn("saz").setType(ColumnType.TIMESTAMP)),
                OperatorType.EQUALS_TO, dateTime));

        final FilterItem filterItem = new FilterItem(children);

        SalesforceDataContext.rewriteFilterItem(sb, filterItem);

        assertEquals(
                "FOOBAR: (foo = 'hello\\n \\'world\\'' OR bar = 123 OR baz = 2013-01-22 OR zaz = 2013-01-22T23:00:00+0000 OR saz = 2013-01-22T23:00:00+0000)",
                sb.toString());
    }
}

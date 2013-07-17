/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.salesforce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.DateUtils;
import org.eobjects.metamodel.util.Month;

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
        cal.setTime(DateUtils.get(2013, Month.JANUARY, 23));
        cal.setTimeZone(TimeZone.getTimeZone("GMT+1"));
        final Date date = cal.getTime();

        final List<FilterItem> children = new ArrayList<FilterItem>();
        children.add(new FilterItem(new SelectItem(new MutableColumn("foo")), OperatorType.EQUALS_TO, "hello\n 'world'"));
        children.add(new FilterItem(new SelectItem(new MutableColumn("bar")), OperatorType.EQUALS_TO, 123));
        children.add(new FilterItem(new SelectItem(new MutableColumn("baz")), OperatorType.EQUALS_TO, date));
        final FilterItem filterItem = new FilterItem(children);

        SalesforceDataContext.rewriteFilterItem(sb, filterItem);

        assertEquals("FOOBAR: (foo = 'hello\\n \\'world\\'' OR bar = 123 OR baz = 2013-01-22T23:00:00+0000)",
                sb.toString());
    }
}

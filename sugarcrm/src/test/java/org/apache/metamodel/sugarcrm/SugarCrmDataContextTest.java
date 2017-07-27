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
package org.apache.metamodel.sugarcrm;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.InMemoryDataSet;
import org.apache.metamodel.data.MaxRowsDataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

import com.sugarcrm.ws.soap.GetEntryListResultVersion2;

public class SugarCrmDataContextTest extends SugarCrmTestCase {

    private static final String BASE_URL = "http://localhost:9090/sugarcrm";

    private SugarCrmDataContext dataContext;

    @Override
    protected void tearDown() throws Exception {
        if (dataContext != null) {
            dataContext.close();
            dataContext = null;
        }
    }

    public void testCountQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        dataContext = new SugarCrmDataContext(BASE_URL, getUsername(), getPassword(), "Test");

        final DataSet ds = dataContext.query().from("Accounts").selectCount().execute();

        assertTrue(ds instanceof InMemoryDataSet);

        assertTrue(ds.next());
        Object count = ds.getRow().getValue(0);
        assertEquals(getNumberOfAccounts(), ((Number) count).intValue());
        assertFalse(ds.next());
        ds.close();
    }

    public void testScrollingQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        dataContext = new SugarCrmDataContext(BASE_URL + "/", getUsername(), getPassword(), "Test");

        final DataSet ds = dataContext.query().from("Accounts").select("name").execute();

        assertNotNull(ds);
        assertTrue("Not a SugarCrmDataSet: " + ds.getClass(), ds instanceof SugarCrmDataSet);

        final GetEntryListResultVersion2 entryList = ((SugarCrmDataSet) ds).getEntryList();
        final int totalCount = entryList.getTotalCount();

        assertTrue(totalCount > 0);
        assertEquals(183, totalCount);

        int counter = 0;
        while (ds.next()) {
            Row row = ds.getRow();
            Object value = row.getValue(0);
            assertNotNull(value);
            assertTrue(value instanceof String);
            assertFalse("".equals(value));
            counter++;
        }

        ds.close();

        assertEquals(counter, totalCount);
    }

    public void testSelectAllColumnsAndTypes() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        dataContext = new SugarCrmDataContext(BASE_URL + "/", getUsername(), getPassword(), "Test");

        final Schema schema = dataContext.getDefaultSchema();
        final Table table = schema.getTableByName("Prospects");
        final List<Column> numberColumns = table.getNumberColumns();
        final List<Column> booleanColumns = table.getBooleanColumns();
        final List<Column> timeBasedColumns = table.getTimeBasedColumns();
        assertTrue(numberColumns.size() > 0);
        assertTrue(booleanColumns.size() > 0);
        assertTrue(timeBasedColumns.size() > 0);

        DataSet ds = dataContext.query().from(table).selectAll().limit(5).execute();
        int rowCounter = 0;
        int numberCounter = 0;
        int booleanCounter = 0;
        int timeBasedCounter = 0;
        while (ds.next()) {
            final Row row = ds.getRow();
            for (Column column : numberColumns) {
                Object value = row.getValue(column);
                if (value != null) {
                    assertTrue(value instanceof Number);
                    numberCounter++;
                }
            }
            for (Column column : booleanColumns) {
                Object value = row.getValue(column);
                if (value != null) {
                    assertTrue(value instanceof Boolean);
                    booleanCounter++;
                }
            }
            for (Column column : timeBasedColumns) {
                Object value = row.getValue(column);
                if (value != null) {
                    assertTrue(value instanceof Date);
                    timeBasedCounter++;
                }
            }
            rowCounter++;
        }
        ds.close();
        assertTrue(rowCounter > 0);
        assertTrue("No non-null values found in: " + Arrays.toString(numberColumns.toArray()), numberCounter > 0);
        assertTrue("No non-null values found in: " + Arrays.toString(booleanColumns.toArray()), booleanCounter > 0);
        assertTrue("No non-null values found in: " + Arrays.toString(timeBasedColumns.toArray()), timeBasedCounter > 0);
    }

    public void testMaxRowsQuery() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        dataContext = new SugarCrmDataContext(BASE_URL + "/", getUsername(), getPassword(), "Test");

        Query query = dataContext.query().from("Employees").select("id", "name").toQuery();
        query.setMaxRows(3);
        final DataSet ds = dataContext.executeQuery(query);

        assertNotNull(ds);
        assertTrue(ds instanceof MaxRowsDataSet);

        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertNotNull(ds.getRow().getValue(1));
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertNotNull(ds.getRow().getValue(1));
        assertTrue(ds.next());
        assertNotNull(ds.getRow().getValue(0));
        assertNotNull(ds.getRow().getValue(1));
        assertFalse(ds.next());
    }

    public void testSchema() throws Exception {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }
        dataContext = new SugarCrmDataContext(BASE_URL, getUsername(), getPassword(), "Test");

        Schema schema = dataContext.getDefaultSchema();
        assertEquals("SugarCRM", schema.getName());

        Table table = schema.getTableByName("Accounts");

        List<String> columnNames = table.getColumnNames();
        assertEquals(
                "[id, name, date_entered, date_modified, modified_user_id, modified_by_name, created_by, created_by_name, "
                        + "description, deleted, assigned_user_id, assigned_user_name, account_type, industry, annual_revenue, phone_fax, "
                        + "billing_address_street, billing_address_street_2, billing_address_street_3, billing_address_street_4, "
                        + "billing_address_city, billing_address_state, billing_address_postalcode, billing_address_country, rating, "
                        + "phone_office, phone_alternate, website, ownership, employees, ticker_symbol, shipping_address_street, "
                        + "shipping_address_street_2, shipping_address_street_3, shipping_address_street_4, shipping_address_city, "
                        + "shipping_address_state, shipping_address_postalcode, shipping_address_country, email1, parent_id, sic_code, parent_name, "
                        + "email_opt_out, invalid_email, email, campaign_id, campaign_name]",
                Arrays.toString(columnNames.toArray()));

        Column nameColumn = table.getColumnByName("name");
        String nativeType = nameColumn.getNativeType();
        String remarks = nameColumn.getRemarks();
        ColumnType type = nameColumn.getType();
        assertEquals("name|Name:|VARCHAR", nativeType + "|" + remarks + "|" + type);

        for (Column column : table.getColumns()) {
            type = column.getType();
            if (type == null || type == ColumnType.OTHER) {
                fail("No type mapping for native type: " + column.getNativeType() + " (found in column: " + column
                        + ")");
            }
        }

        table = schema.getTableByName("Contacts");

        columnNames = table.getColumnNames();
        assertEquals(
                "[id, name, date_entered, date_modified, modified_user_id, modified_by_name, created_by, created_by_name, description, deleted, "
                        + "assigned_user_id, assigned_user_name, salutation, first_name, last_name, full_name, title, department, do_not_call, phone_home, "
                        + "email, phone_mobile, phone_work, phone_other, phone_fax, email1, email2, invalid_email, email_opt_out, primary_address_street, "
                        + "primary_address_street_2, primary_address_street_3, primary_address_city, primary_address_state, primary_address_postalcode, "
                        + "primary_address_country, alt_address_street, alt_address_street_2, alt_address_street_3, alt_address_city, alt_address_state, "
                        + "alt_address_postalcode, alt_address_country, assistant, assistant_phone, email_and_name1, lead_source, account_name, account_id, "
                        + "opportunity_role_fields, opportunity_role_id, opportunity_role, reports_to_id, report_to_name, birthdate, campaign_id, campaign_name, "
                        + "c_accept_status_fields, m_accept_status_fields, accept_status_id, accept_status_name, sync_contact]",
                Arrays.toString(columnNames.toArray()));

        for (Column column : table.getColumns()) {
            type = column.getType();
            if (type == null || type == ColumnType.OTHER) {
                fail("No type mapping for native type: " + column.getNativeType() + " (found in column: " + column
                        + ")");
            }
        }
    }
}

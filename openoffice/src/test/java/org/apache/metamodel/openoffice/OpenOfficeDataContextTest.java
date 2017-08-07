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
package org.apache.metamodel.openoffice;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

import javax.swing.table.TableModel;

import junit.framework.TestCase;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetTableModel;
import org.apache.metamodel.openoffice.OpenOfficeDataContext;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class OpenOfficeDataContextTest extends TestCase {

	private final static File DB_FILE = new File(
			"src/test/resources/openoffice_db.odb");
	private OpenOfficeDataContext _dataContext;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		_dataContext = new OpenOfficeDataContext(DB_FILE);
	}

	public void testUnpacking() throws Exception {
		assertTrue(DB_FILE.exists());

		Connection connection = _dataContext.getConnection();
		assertNotNull(connection);
		assertEquals("HSQL Database Engine", connection.getMetaData()
				.getDatabaseProductName());

		connection.close();
	}

	public void testGetSchemas() throws Exception {
		assertEquals("[INFORMATION_SCHEMA, PUBLIC]",
				Arrays.toString(_dataContext.getSchemaNames().toArray()));

		Schema informationSchema = _dataContext
				.getSchemaByName("INFORMATION_SCHEMA");
		assertEquals(0, informationSchema.getTableCount());

		Schema schema = _dataContext.getDefaultSchema();
		assertEquals("PUBLIC", schema.getName());

		assertEquals(
				"[Table[name=CONTRIBUTORS,type=TABLE,remarks=null], Table[name=projects,type=TABLE,remarks=null]]",
				Arrays.toString(schema.getTables().toArray()));
		assertEquals(
				"[Relationship[primaryTable=CONTRIBUTORS,primaryColumns=[ID],foreignTable=projects,foreignColumns=[admin]]]",
				Arrays.toString(schema.getRelationships().toArray()));
	}

	public void testQueryUppercaseTable() throws Exception {
		Schema schema = _dataContext.getDefaultSchema();
		Table contributorsTable = schema.getTableByName("CONTRIBUTORS");

		Query q = new Query().from(contributorsTable).select(
				contributorsTable.getColumns());

		assertEquals(
				"SELECT \"CONTRIBUTORS\".\"ID\", \"CONTRIBUTORS\".\"USERNAME\", \"CONTRIBUTORS\".\"ROLE\", \"CONTRIBUTORS\".\"COUNTRY\" FROM PUBLIC.\"CONTRIBUTORS\"",
				q.toString());

		DataSet data = _dataContext.executeQuery(q);
		TableModel tableModel = new DataSetTableModel(data);
		assertEquals(4, tableModel.getColumnCount());
		assertEquals(4, tableModel.getRowCount());
	}

	public void testQueryLowercaseTable() throws Exception {
		Schema schema = _dataContext.getDefaultSchema();
		Table projectsTable = schema.getTableByName("projects");

		Query q = new Query().from(projectsTable).select(
				projectsTable.getColumns());

		assertEquals(
				"SELECT \"projects\".\"ID\", \"projects\".\"project\", \"projects\".\"type\", \"projects\".\"admin\" FROM PUBLIC.\"projects\"",
				q.toString());

		DataSet data = _dataContext.executeQuery(q);
		TableModel tableModel = new DataSetTableModel(data);
		assertEquals(4, tableModel.getColumnCount());
		assertEquals(3, tableModel.getRowCount());
	}
}
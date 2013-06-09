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

package org.eobjects.metamodel.openoffice;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

import javax.swing.table.TableModel;

import junit.framework.TestCase;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DataSetTableModel;
import org.eobjects.metamodel.openoffice.OpenOfficeDataContext;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

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
				Arrays.toString(_dataContext.getSchemaNames()));

		Schema informationSchema = _dataContext
				.getSchemaByName("INFORMATION_SCHEMA");
		assertEquals(0, informationSchema.getTableCount());

		Schema schema = _dataContext.getDefaultSchema();
		assertEquals("PUBLIC", schema.getName());

		assertEquals(
				"[Table[name=CONTRIBUTORS,type=TABLE,remarks=null], Table[name=projects,type=TABLE,remarks=null]]",
				Arrays.toString(schema.getTables()));
		assertEquals(
				"[Relationship[primaryTable=CONTRIBUTORS,primaryColumns=[ID],foreignTable=projects,foreignColumns=[admin]]]",
				Arrays.toString(schema.getRelationships()));
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
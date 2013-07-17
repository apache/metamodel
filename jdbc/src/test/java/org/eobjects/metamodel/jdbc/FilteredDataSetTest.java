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

package org.eobjects.metamodel.jdbc;

import java.sql.Connection;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.FilteredDataSet;
import org.eobjects.metamodel.data.IRowFilter;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Table;

public class FilteredDataSetTest extends JdbcTestCase {

	public void testStrategy() throws Exception {
		Connection connection = getTestDbConnection();
		DataContext dc = new JdbcDataContext(connection);
		Table customerTable = dc.getDefaultSchema().getTableByName("CUSTOMERS");

		Query q = new Query().from(customerTable).select(
				customerTable.getColumns());
		DataSet dataSet = dc.executeQuery(q);
		IRowFilter filter1 = new IRowFilter() {

			public boolean accept(Row row) {
				// This will only accept five records
				boolean result = ((Integer) row.getValue(0)) > 485;
				System.out.println("Evaluating: " + row);
				System.out.println("Accepting: " + result);
				return result;
			}

		};
		IRowFilter filter2 = new IRowFilter() {
			int count = 0;

			public boolean accept(Row row) {
				count++;
				return (count == 2 || count == 3);
			}

		};
		dataSet = new FilteredDataSet(dataSet, filter1, filter2);
		assertTrue(dataSet.next());
		assertEquals(
				"Row[values=[487, Signal Collectibles Ltd., Taylor, Sue, 4155554312, 2793 Furth Circle, null, Brisbane, CA, 94217, USA, 1165, 60300.0]]",
				dataSet.getRow().toString());
		assertTrue(dataSet.next());
		assertEquals(
				"Row[values=[489, Double Decker Gift Stores, Ltd, Hardy, Thomas, (171) 555-7555, 120 Hanover Sq., null, London, null, WA1 1DP, UK, 1501, 43300.0]]",
				dataSet.getRow().toString());
		assertFalse(dataSet.next());
		
		dataSet.close();
	}
}
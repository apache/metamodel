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

import junit.framework.TestCase;

import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;

public class FetchSizeCalculatorTest extends TestCase {

	public void testGetFetchSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(8 * 1024 * 1024);
		assertEquals(10922, calc.getFetchSize(new MutableColumn("a",
				ColumnType.VARCHAR).setColumnSize(256), new MutableColumn("b",
				ColumnType.VARCHAR).setColumnSize(128)));

		assertEquals(2048, calc.getFetchSize(new MutableColumn("a",
				ColumnType.BLOB).setColumnSize(4096)));
	}

	public void testSingleRowQuery() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(8 * 1024 * 1024);
		assertEquals(1, calc.getFetchSize(new Query().selectCount().from(
				new MutableTable("foo"))));
	}

	public void testMinimumSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(8);

		assertEquals(1, calc.getFetchSize(new MutableColumn("a",
				ColumnType.INTEGER).setColumnSize(256)));
	}

	public void testMaximumSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(Integer.MAX_VALUE);

		assertEquals(25000, calc.getFetchSize(new MutableColumn("a",
				ColumnType.BOOLEAN).setColumnSize(1)));
	}

	public void testManyColumnsScenario() throws Exception {
		Column[] cols = new Column[110];
		for (int i = 0; i < cols.length; i++) {
			MutableColumn col = new MutableColumn("foo" + i);
			if (i % 5 == 0) {
				col.setType(ColumnType.INTEGER);
			} else if (i % 4 == 0) {
				col.setType(ColumnType.BOOLEAN);
			} else if (i % 3 == 0) {
				col.setType(ColumnType.DATE);
			} else {
				col.setType(ColumnType.VARCHAR);
			}

			col.setColumnSize(50);
			cols[i] = col;
		}

		FetchSizeCalculator calc = new FetchSizeCalculator(32 * 1024 * 1024);

		assertEquals(613, calc.getFetchSize(cols));
	}

	public void testGetValueSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(0);

		assertEquals(30, calc.getValueSize(SelectItem.getCountAllItem()));

		assertEquals(30,
				calc.getValueSize(new MutableColumn("", ColumnType.ARRAY)));
		assertEquals(300, calc.getValueSize(new MutableColumn("",
				ColumnType.ARRAY).setColumnSize(10)));

		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.INTEGER)));
		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.BIGINT)));
		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.DOUBLE)));
		assertEquals(16,
				calc.getValueSize(new MutableColumn("", ColumnType.FLOAT)));

		assertEquals(4096,
				calc.getValueSize(new MutableColumn("", ColumnType.CLOB)));
		assertEquals(4096,
				calc.getValueSize(new MutableColumn("", ColumnType.BLOB)));

		assertEquals(4096, calc.getValueSize(new MutableColumn("",
				ColumnType.CLOB).setColumnSize(20)));
		assertEquals(200000, calc.getValueSize(new MutableColumn("",
				ColumnType.BLOB).setColumnSize(200000)));
		assertEquals(400000, calc.getValueSize(new MutableColumn("",
				ColumnType.CLOB).setColumnSize(200000)));
	}

	public void testGetRowSize() throws Exception {
		FetchSizeCalculator calc = new FetchSizeCalculator(0);

		assertEquals(8000, calc.getRowSize(new MutableColumn("",
				ColumnType.VARCHAR).setColumnSize(4000)));
		assertEquals(1024,
				calc.getRowSize(new MutableColumn("", ColumnType.VARCHAR)));

		assertEquals(9024, calc.getRowSize(new MutableColumn("",
				ColumnType.VARCHAR).setColumnSize(4000), new MutableColumn("",
				ColumnType.VARCHAR)));
	}
}

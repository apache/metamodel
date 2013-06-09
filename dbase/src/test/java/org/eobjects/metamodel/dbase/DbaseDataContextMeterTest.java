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
package org.eobjects.metamodel.dbase;

import java.util.Arrays;
import java.util.Date;

import junit.framework.TestCase;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.DateUtils;
import org.eobjects.metamodel.util.Month;

public class DbaseDataContextMeterTest extends TestCase {

	private DbaseDataContext dc;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		dc = new DbaseDataContext("src/test/resources/METER.DBF");
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		dc.close();
	}

	public void testExploreSchema() throws Exception {
		assertEquals("[information_schema, METER.DBF]",
				Arrays.toString(dc.getSchemaNames()));

		Schema schema = dc.getSchemaByName("METER.DBF");
		assertEquals("[Table[name=METER,type=TABLE,remarks=null]]",
				Arrays.toString(schema.getTables()));

		Table table = schema.getTableByName("METER");
		assertEquals(
				"[SCENARIO, P_INST, S_INST, A_INST, R_MULT, M_INST, G_BEG, T_BEG, G_NUM, A_NORM, F_KIND, F_TYPE, F_TOU, DATE_N, UTILCALC, DATE_D, UTIL, NAME, U_NAME, U_KIND, U_UNIT, N_UNIT, C_NAME, P_NAME, S_NAME, A_NAME, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, N1, N2, N3, N4, N5, N6, N7, N8, N9, N10, N11, N12, N_COUNT]",
				Arrays.toString(table.getColumnNames()));

		Column column = table.getColumnByName("F_TYPE");
		assertEquals(
				"Column[name=F_TYPE,columnNumber=11,type=CHAR,nullable=null,nativeType=C,columnSize=1]",
				column.toString());

		column = table.getColumnByName("V1");
		assertEquals(
				"Column[name=V1,columnNumber=26,type=DOUBLE,nullable=null,nativeType=N,columnSize=12]",
				column.toString());

		column = table.getColumnByName("V12");
		assertEquals(
				"Column[name=V12,columnNumber=37,type=DOUBLE,nullable=null,nativeType=N,columnSize=12]",
				column.toString());

		column = table.getColumnByName("A_NAME");
		assertEquals(
				"Column[name=A_NAME,columnNumber=25,type=CHAR,nullable=null,nativeType=C,columnSize=22]",
				column.toString());
	}

	public void testQuery() throws Exception {
		Schema schema = dc.getSchemaByName("METER.DBF");
		Table table = schema.getTableByName("METER");

		Column[] columns = table.getColumns();
		Query q = new Query().select(columns).from(table);
		assertEquals(ColumnType.CHAR, q.getSelectClause().getItem(0)
				.getColumn().getType());
		assertEquals(ColumnType.CHAR, q.getSelectClause().getItem(6)
				.getColumn().getType());
		assertEquals(ColumnType.CHAR, q.getSelectClause().getItem(7)
				.getColumn().getType());
		assertEquals(ColumnType.CHAR, q.getSelectClause().getItem(8)
				.getColumn().getType());
		assertEquals(ColumnType.CHAR, q.getSelectClause().getItem(9)
				.getColumn().getType());
		assertEquals(ColumnType.DOUBLE, q.getSelectClause().getItem(30)
				.getColumn().getType());
		DataSet ds = dc.executeQuery(q);

		assertTrue(ds.next());

		// create a cross-locale string comparison of the date field
		final String dateString;
		{
			Date date = DateUtils.get(2009, Month.AUGUST, 31);
			dateString = date.toString();
		}

		assertEquals(
				"Row[values=[0002, 0001, 0001, 0001, 0001, 0001, 21, 2008,   -1, N, C, A, 0, 149627, A, "
						+ dateString
						+ ", E, NRC E1                , Electric  , Cost      , $             ,               , SimActual             , Parkwood Hospital     , Parkwood Hospital     , Neuro Rehab Centre    , 2893.26, 2180.46, 4541.75, 2894.24, 2981.31, 2702.11, 2733.67, 2733.67, 2597.37, 2850.39, 2914.74, 2951.58, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 12.0]]",
				ds.getRow().toString());
		assertEquals(Double.class, ds.getRow().getValue(30).getClass());

		while (ds.next()) {
			Row row = ds.getRow();
			SelectItem[] selectItems = row.getSelectItems();
			for (int i = 0; i < selectItems.length; i++) {
				SelectItem selectItem = selectItems[i];
				Column column = columns[i];
				assertSame(selectItem.getColumn(), column);

				Object selectItemValue = row.getValue(selectItem);
				Object columnValue = row.getValue(column);
				assertEquals(selectItemValue, columnValue);

				assertNotNull(columnValue);

				if (column.getType() == ColumnType.CHAR) {
					assertTrue(columnValue instanceof Character
							|| columnValue instanceof String);
				} else if (column.getType() == ColumnType.FLOAT) {
					assertTrue(columnValue instanceof Float);
				} else if (column.getType() == ColumnType.DOUBLE) {
					assertTrue(columnValue instanceof Double);
				} else if (column.getType() == ColumnType.DATE) {
					assertTrue(columnValue instanceof Date);
				} else if (column.getType() == ColumnType.OTHER) {
					System.out.println("other type: " + columnValue);
				} else {
					throw new MetaModelException(
							"Value type not expected for Dbase data: " + column);
				}
			}
		}
		ds.close();
	}
}
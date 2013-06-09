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

package org.eobjects.metamodel.schema;

import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

public class ColumnTypeTest extends TestCase {

	public void testConvertColumnTypeFromJdbcTypes() throws Exception {
		ColumnType type = ColumnType.convertColumnType(Types.VARCHAR);
		assertEquals(ColumnType.VARCHAR, type);
		
		type = ColumnType.convertColumnType(Types.DATE);
        assertEquals(ColumnType.DATE, type);

		type = ColumnType.convertColumnType(Types.TIME);
		assertEquals(ColumnType.TIME, type);

		type = ColumnType.convertColumnType(Types.TIMESTAMP);
		assertEquals(ColumnType.TIMESTAMP, type);

		type = ColumnType.convertColumnType(42397443);
		assertEquals(ColumnType.OTHER, type);
		
		type = ColumnType.convertColumnType(-42397443);
		assertEquals(ColumnType.OTHER, type);
	}
	
	public void testConvertColumnTypeFromJavaClass() throws Exception {
		ColumnType type = ColumnType.convertColumnType(String.class);
		assertEquals(ColumnType.VARCHAR, type);

		type = ColumnType.convertColumnType(Time.class);
		assertEquals(ColumnType.TIME, type);

		type = ColumnType.convertColumnType(Timestamp.class);
		assertEquals(ColumnType.TIMESTAMP, type);
		
		type = ColumnType.convertColumnType(java.sql.Date.class);
		assertEquals(ColumnType.DATE, type);

		type = ColumnType.convertColumnType(Date.class);
		assertEquals(ColumnType.TIMESTAMP, type);
		
		type = ColumnType.convertColumnType(Integer.class);
		assertEquals(ColumnType.INTEGER, type);
		
		type = ColumnType.convertColumnType(Object.class);
		assertEquals(ColumnType.OTHER, type);
		
		type = ColumnType.convertColumnType(Map.class);
		assertEquals(ColumnType.MAP, type);
		type = ColumnType.convertColumnType(HashMap.class);
		assertEquals(ColumnType.MAP, type);
		type = ColumnType.convertColumnType(TreeMap.class);
		assertEquals(ColumnType.MAP, type);
		
		type = ColumnType.convertColumnType(List.class);
		assertEquals(ColumnType.LIST, type);
		type = ColumnType.convertColumnType(ArrayList.class);
		assertEquals(ColumnType.LIST, type);
		type = ColumnType.convertColumnType(LinkedList.class);
		assertEquals(ColumnType.LIST, type);
	}
}
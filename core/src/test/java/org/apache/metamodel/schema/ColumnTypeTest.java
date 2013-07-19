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
package org.apache.metamodel.schema;

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
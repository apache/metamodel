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

import junit.framework.TestCase;

public class TableTypeTest extends TestCase {

	public void testGetTableType() throws Exception {
		assertSame(TableType.TABLE, TableType.getTableType("table"));
		assertSame(TableType.VIEW, TableType.getTableType("view"));
		assertSame(TableType.GLOBAL_TEMPORARY, TableType
				.getTableType("GLOBAL_TEMPORARY"));
		assertSame(TableType.SYSTEM_TABLE, TableType
				.getTableType("system_table"));
		assertSame(TableType.LOCAL_TEMPORARY, TableType
				.getTableType("LOCAL_TEMPORARY"));
		assertSame(TableType.ALIAS, TableType.getTableType("alIAs"));
		assertSame(TableType.SYNONYM, TableType.getTableType("synonym"));
		assertSame(TableType.OTHER, TableType.getTableType("foobar"));
	}
}
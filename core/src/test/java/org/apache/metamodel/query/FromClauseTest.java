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
package org.apache.metamodel.query;

import org.apache.metamodel.MetaModelTestCase;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

public class FromClauseTest extends MetaModelTestCase {

	public void testGetItemByReference() throws Exception {
		Schema exampleSchema = getExampleSchema();
		Table table = exampleSchema.getTableByName(TABLE_CONTRIBUTOR);

		Query query = new Query();
		query.from(table, "foobar");

		assertNull(query.getFromClause().getItemByReference("foob"));
		assertNull(query.getFromClause().getItemByReference(TABLE_CONTRIBUTOR));
		assertEquals("MetaModelSchema.contributor foobar", query
				.getFromClause().getItemByReference("foobar").toString());

		query = new Query();
		query.from(table);
		assertNull(query.getFromClause().getItemByReference("foob"));
		assertEquals("MetaModelSchema.contributor", query.getFromClause()
				.getItemByReference(TABLE_CONTRIBUTOR).toString());
		assertNull(query.getFromClause().getItemByReference("foobar"));
	}
}
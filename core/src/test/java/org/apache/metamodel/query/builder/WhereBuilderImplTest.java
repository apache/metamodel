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
package org.apache.metamodel.query.builder;

import java.util.Arrays;
import java.util.Collection;

import org.apache.metamodel.query.Query;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import junit.framework.TestCase;

public class WhereBuilderImplTest extends TestCase {

	private MutableColumn col1 = new MutableColumn("col1", ColumnType.BOOLEAN);
	private MutableColumn col2 = new MutableColumn("col2");
	private WhereBuilderImpl whereBuilder;
	private Query query;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		query = new Query();
		GroupedQueryBuilder queryBuilder = new GroupedQueryBuilderImpl(null,
				query);
		whereBuilder = new WhereBuilderImpl(col1, query, queryBuilder);
	}

	public void testOr() throws Exception {
		whereBuilder.eq(true).or(col2).like("%testcase%");

		assertEquals(" WHERE (col1 = TRUE OR col2 LIKE '%testcase%')",
				query.toSql());
	}

	public void testNotLike() throws Exception {
		whereBuilder.eq(true).or(col2).notLike("%test%case%");

		assertEquals(" WHERE (col1 = TRUE OR col2 NOT LIKE '%test%case%')",
				query.toSql());
	}

	public void testAnd() throws Exception {
		whereBuilder.differentFrom(true).and(col2).eq(1).or(col2).eq(2)
				.or(col2).eq(3).and(new MutableColumn("col3")).eq(4);

		assertEquals(
				" WHERE col1 <> TRUE AND (col2 = 1 OR col2 = 2 OR col2 = 3) AND col3 = 4",
				query.toSql());
	}

	public void testInStringArray() throws Exception {
		whereBuilder.eq(true).or(col2).in("foo", "bar");

		assertEquals(" WHERE (col1 = TRUE OR col2 IN ('foo' , 'bar'))",
				query.toSql());
	}

	public void testNotInStringArray() throws Exception {
		whereBuilder.eq(true).or(col2).notIn("foo", "bar");

		assertEquals(" WHERE (col1 = TRUE OR col2 NOT IN ('foo' , 'bar'))",
				query.toSql());
	}

	public void testInNumberArray() throws Exception {
		whereBuilder.eq(true).or(col2).in(3, 1);

		assertEquals(" WHERE (col1 = TRUE OR col2 IN (3 , 1))", query.toSql());
	}

	public void testNotInNumberArray() throws Exception {
		whereBuilder.eq(true).or(col2).notIn(3, 1);

		assertEquals(" WHERE (col1 = TRUE OR col2 NOT IN (3 , 1))", query.toSql());
	}

	public void testInCollection() throws Exception {
		Collection<?> col = Arrays.asList("foo", "bar");
		whereBuilder.eq(true).or(col2).in(col);

		assertEquals(" WHERE (col1 = TRUE OR col2 IN ('foo' , 'bar'))",
				query.toSql());
	}

	public void testNotInCollection() throws Exception {
		Collection<?> col = Arrays.asList("foo", "bar");
		whereBuilder.eq(true).or(col2).notIn(col);

		assertEquals(" WHERE (col1 = TRUE OR col2 NOT IN ('foo' , 'bar'))",
				query.toSql());
	}
}
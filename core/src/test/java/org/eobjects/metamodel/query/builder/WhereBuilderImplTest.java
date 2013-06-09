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
package org.eobjects.metamodel.query.builder;

import java.util.Arrays;
import java.util.Collection;

import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
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

	public void testInNumberArray() throws Exception {
		whereBuilder.eq(true).or(col2).in(3, 1);

		assertEquals(" WHERE (col1 = TRUE OR col2 IN (3 , 1))", query.toSql());
	}

	public void testInCollection() throws Exception {
		Collection<?> col = Arrays.asList("foo", "bar");
		whereBuilder.eq(true).or(col2).in(col);

		assertEquals(" WHERE (col1 = TRUE OR col2 IN ('foo' , 'bar'))",
				query.toSql());
	}
}
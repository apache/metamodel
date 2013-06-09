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
package org.eobjects.metamodel.query;

import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Table;

import junit.framework.TestCase;

public class SelectClauseTest extends TestCase {

	public void testDistinctAddition() throws Exception {
		Table table = new MutableTable("foo");
		Column col = new MutableColumn("bar").setTable(table);

		Query q = new Query();
		q.selectDistinct();
		q.from(table);
		q.select(col);

		assertEquals("SELECT DISTINCT foo.bar FROM foo", q.toSql());
	}
}

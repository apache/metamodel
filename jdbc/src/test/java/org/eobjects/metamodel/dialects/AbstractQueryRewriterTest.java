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
package org.eobjects.metamodel.dialects;

import junit.framework.TestCase;

import org.eobjects.metamodel.jdbc.dialects.AbstractQueryRewriter;
import org.eobjects.metamodel.jdbc.dialects.DefaultQueryRewriter;
import org.eobjects.metamodel.query.FilterItem;
import org.eobjects.metamodel.query.FromItem;
import org.eobjects.metamodel.query.OperatorType;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableTable;

public class AbstractQueryRewriterTest extends TestCase {

    public void testRewriteQuery() throws Exception {
        Query q = new Query().selectCount().from(new MutableTable("foobar"))
                .where(new MutableColumn("foob"), OperatorType.EQUALS_TO, null).groupBy(new MutableColumn("col1"))
                .having(new FilterItem(new SelectItem(new MutableColumn("col2")), OperatorType.GREATER_THAN, 40))
                .orderBy(new MutableColumn("bla"));
        assertEquals("SELECT COUNT(*) FROM foobar WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                q.toString());

        AbstractQueryRewriter rewriter = new DefaultQueryRewriter(null) {

        };

        assertEquals("SELECT COUNT(*) FROM foobar WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                rewriter.rewriteQuery(q));

        rewriter = new DefaultQueryRewriter(null) {
            @Override
            protected String rewriteFromItem(Query query, FromItem item) {
                return "mytable";
            }
        };

        assertEquals("SELECT COUNT(*) FROM mytable WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                rewriter.rewriteQuery(q));

        q.getSelectClause().setDistinct(true);

        assertEquals(
                "SELECT DISTINCT COUNT(*) FROM mytable WHERE foob IS NULL GROUP BY col1 HAVING col2 > 40 ORDER BY bla ASC",
                rewriter.rewriteQuery(q));
    }
}
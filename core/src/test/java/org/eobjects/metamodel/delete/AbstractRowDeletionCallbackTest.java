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
package org.eobjects.metamodel.delete;

import junit.framework.TestCase;

import org.eobjects.metamodel.MockUpdateableDataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.schema.Table;

public class AbstractRowDeletionCallbackTest extends TestCase {

    public void testDelete() throws Exception {
        final MockUpdateableDataContext dc = new MockUpdateableDataContext();
        final Table table = dc.getDefaultSchema().getTables()[0];
        DataSet ds = dc.query().from(table).selectCount().execute();
        assertTrue(ds.next());
        assertEquals("3", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                callback.update(table).value("bar", "baz").execute();
                callback.update(table).value("foo", "4").where("foo").eq("3").execute();
            }
        });

        ds = dc.query().from(table).select(table.getColumns()).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[1, baz]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[2, baz]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[4, baz]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        dc.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                RowDeletionBuilder delete = callback.deleteFrom(table);
                assertEquals("DELETE FROM schema.table", delete.toSql());
                delete.execute();

                assertEquals("DELETE FROM schema.table WHERE table.bar = 'baz'", callback.deleteFrom(table).where("bar")
                        .eq("baz").toSql());
            }
        });

        ds = dc.query().from(table).selectCount().execute();
        assertTrue(ds.next());
        assertEquals("0", ds.getRow().getValue(0).toString());
        assertFalse(ds.next());
        ds.close();
    }
}

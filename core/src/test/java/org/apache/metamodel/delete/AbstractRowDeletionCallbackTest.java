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
package org.apache.metamodel.delete;

import junit.framework.TestCase;

import org.apache.metamodel.MockUpdateableDataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;

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

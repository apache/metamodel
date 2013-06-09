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
package org.eobjects.metamodel.util;

import java.io.InputStream;
import java.io.OutputStream;

import junit.framework.TestCase;

public class InMemoryResourceTest extends TestCase {

    public void testScenario() throws Exception {
        InMemoryResource r = new InMemoryResource("foo/bar");
        assertEquals("bar", r.getName());
        assertEquals(-1, r.getLastModified());
        assertEquals(0, r.getSize());
        assertFalse(r.isReadOnly());
        assertTrue(r.isExists());

        r.write(new Action<OutputStream>() {
            @Override
            public void run(OutputStream out) throws Exception {
                out.write(1);
                out.write(2);
                out.write(3);
            }
        });

        assertEquals(3, r.getSize());

        r.read(new Action<InputStream>() {
            @Override
            public void run(InputStream in) throws Exception {
                assertEquals(1, in.read());
                assertEquals(2, in.read());
                assertEquals(3, in.read());
                assertEquals(-1, in.read());
            }
        });

        r.append(new Action<OutputStream>() {
            @Override
            public void run(OutputStream out) throws Exception {
                out.write(4);
                out.write(5);
                out.write(6);
            }
        });

        r.read(new Action<InputStream>() {
            @Override
            public void run(InputStream in) throws Exception {
                assertEquals(1, in.read());
                assertEquals(2, in.read());
                assertEquals(3, in.read());
                assertEquals(4, in.read());
                assertEquals(5, in.read());
                assertEquals(6, in.read());
                assertEquals(-1, in.read());
            }
        });
    }
}

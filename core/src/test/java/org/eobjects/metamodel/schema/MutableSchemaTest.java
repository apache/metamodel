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

package org.eobjects.metamodel.schema;

import junit.framework.TestCase;

public class MutableSchemaTest extends TestCase {

    /**
     * Tests that the following (general) rules apply to the object:
     * 
     * <li>the hashcode is the same when run twice on an unaltered object</li>
     * <li>if o1.equals(o2) then this condition must be true: o1.hashCode() ==
     * 02.hashCode()
     */
    public void testEqualsAndHashCode() throws Exception {
        MutableSchema schema1 = new MutableSchema("foo");
        MutableSchema schema2 = new MutableSchema("foo");

        assertTrue(schema1.equals(schema2));
        assertTrue(schema1.hashCode() == schema2.hashCode());

        schema2.addTable(new MutableTable("foo"));
        assertFalse(schema1.equals(schema2));
        assertTrue(schema1.hashCode() == schema2.hashCode());

        schema2 = new MutableSchema("foo");
        assertTrue(schema1.equals(schema2));
        assertTrue(schema1.hashCode() == schema2.hashCode());
    }

    public void testGetTableByName() throws Exception {
        MutableSchema s = new MutableSchema("foobar");
        s.addTable(new MutableTable("Foo"));
        s.addTable(new MutableTable("FOO"));
        s.addTable(new MutableTable("bar"));

        assertEquals("Foo", s.getTableByName("Foo").getName());
        assertEquals("FOO", s.getTableByName("FOO").getName());
        assertEquals("bar", s.getTableByName("bar").getName());

        // picking the first alternative that matches case insensitively
        assertEquals("Foo", s.getTableByName("fOO").getName());
    }
}
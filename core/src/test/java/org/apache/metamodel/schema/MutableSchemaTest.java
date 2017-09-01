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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;

import org.apache.metamodel.util.LegacyDeserializationObjectInputStream;
import org.junit.Test;

public class MutableSchemaTest {

    /**
     * Tests that the following (general) rules apply to the object:
     * 
     * <li>the hashcode is the same when run twice on an unaltered object</li>
     * <li>if o1.equals(o2) then this condition must be true: o1.hashCode() ==
     * 02.hashCode()
     */
    @Test
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

    @Test
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

    @Test
    public void testDeserializeOldFormat() throws Exception {
        final File file = new File("src/test/resources/metamodel-4.6.0-mutableschema-etc.ser");
        assertTrue(file.exists());

        try (final FileInputStream in = new FileInputStream(file)) {
            final LegacyDeserializationObjectInputStream ois = new LegacyDeserializationObjectInputStream(in);
            final Object obj = ois.readObject();
            assertTrue(obj instanceof MutableSchema);
            ois.close();

            final MutableSchema sch = (MutableSchema) obj;
            assertEquals("schema", sch.getName());

            assertEquals(2, sch.getTableCount());

            final Table table1 = sch.getTable(0);
            assertTrue(table1 instanceof MutableTable);
            assertEquals("t1", table1.getName());
            assertEquals(Arrays.asList("t1_c1", "t1_c2"), table1.getColumnNames());
            assertEquals(1, table1.getRelationshipCount());

            final Table table2 = sch.getTable(1);
            assertTrue(table2 instanceof MutableTable);
            assertEquals("t2", table2.getName());
            assertEquals(Arrays.asList("t2_c1"), table2.getColumnNames());
            assertEquals(1, table2.getRelationshipCount());

            final Relationship rel1 = table1.getRelationships().iterator().next();
            final Relationship rel2 = table2.getRelationships().iterator().next();
            assertSame(rel1, rel2);
            assertTrue(rel1 instanceof MutableRelationship);
        }
    }
}
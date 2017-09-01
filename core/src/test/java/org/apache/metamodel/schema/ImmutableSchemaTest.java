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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;

import org.apache.metamodel.MetaModelTestCase;
import org.apache.metamodel.util.LegacyDeserializationObjectInputStream;
import org.junit.Test;

public class ImmutableSchemaTest {

    @Test
    public void testConstructor() throws Exception {
        Schema mutableSchema = MetaModelTestCase.getExampleSchema();
        assertTrue(mutableSchema instanceof MutableSchema);

        ImmutableSchema immutableSchema = new ImmutableSchema(mutableSchema);

        assertEquals(mutableSchema.getRelationshipCount(), immutableSchema.getRelationshipCount());

        assertEquals(immutableSchema, mutableSchema);
    }

    @Test
    public void testDeserializeOldFormat() throws Exception {
        final File file = new File("src/test/resources/metamodel-4.6.0-immutableschema-etc.ser");
        assertTrue(file.exists());

        try (final FileInputStream in = new FileInputStream(file)) {
            final LegacyDeserializationObjectInputStream ois = new LegacyDeserializationObjectInputStream(in);
            final Object obj = ois.readObject();
            assertTrue(obj instanceof ImmutableSchema);
            ois.close();

            final ImmutableSchema sch = (ImmutableSchema) obj;
            assertEquals("schema", sch.getName());

            assertEquals(2, sch.getTableCount());

            final Table table1 = sch.getTable(0);
            assertTrue(table1 instanceof ImmutableTable);
            assertEquals("t1", table1.getName());
            assertEquals(Arrays.asList("t1_c1", "t1_c2"), table1.getColumnNames());
            assertEquals(1, table1.getRelationshipCount());

            final Table table2 = sch.getTable(1);
            assertTrue(table2 instanceof ImmutableTable);
            assertEquals("t2", table2.getName());
            assertEquals(Arrays.asList("t2_c1"), table2.getColumnNames());
            assertEquals(1, table2.getRelationshipCount());

            final Relationship rel1 = table1.getRelationships().iterator().next();
            final Relationship rel2 = table2.getRelationships().iterator().next();
            assertSame(rel1, rel2);
            assertTrue(rel1 instanceof ImmutableRelationship);
        }
    }
}
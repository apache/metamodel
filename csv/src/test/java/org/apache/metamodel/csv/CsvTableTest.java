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
package org.apache.metamodel.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.LegacyDeserializationObjectInputStream;
import org.junit.Test;

public class CsvTableTest {

    @Test
    public void testDeserializeOldTable() throws Exception {
        final File file = new File("src/test/resources/MetaModel-4.6.0-CsvTable.ser");
        try (LegacyDeserializationObjectInputStream in =
                new LegacyDeserializationObjectInputStream(new FileInputStream(file))) {
            final Object object = in.readObject();

            assertPeopleCsv(object);
        }
    }

    @Test
    public void testSerializeAndDeserializeCurrentVersion() throws Exception {
        final DataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"));
        final Table table1 = dc.getDefaultSchema().getTables().get(0);
        assertPeopleCsv(table1);

        final byte[] bytes = SerializationUtils.serialize(table1);

        try (LegacyDeserializationObjectInputStream in =
                new LegacyDeserializationObjectInputStream(new ByteArrayInputStream(bytes))) {
            final Object object = in.readObject();

            assertPeopleCsv(object);
        }
    }

    private void assertPeopleCsv(Object object) {
        assertTrue(object instanceof CsvTable);

        final Table table = (Table) object;

        assertEquals("csv_people.csv", table.getName());

        final Schema schema = table.getSchema();
        assertEquals("resources", schema.getName());
        assertEquals(1, schema.getTables().size());
        assertEquals(table, schema.getTable(0));

        assertEquals("[id, name, gender, age]", table.getColumnNames().toString());
    }
}

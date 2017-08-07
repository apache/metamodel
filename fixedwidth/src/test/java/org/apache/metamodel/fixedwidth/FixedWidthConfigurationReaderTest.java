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
package org.apache.metamodel.fixedwidth;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.junit.Test;

public class FixedWidthConfigurationReaderTest {

    private final FileResource dataResource = new FileResource("src/test/resources/metadata_spec1/data.txt");

    @Test
    public void testReadConfigurationFromSasFormatFile() throws Exception {
        final FixedWidthConfigurationReader reader = new FixedWidthConfigurationReader();
        final Resource resource = new FileResource("src/test/resources/metadata_spec1/sas-formatfile-metadata.txt");
        assertTrue(resource.isExists());

        final FixedWidthConfiguration configuration = reader.readFromSasFormatFile("UTF8", resource, false);
        assertEquals("[1, 20, 2]", Arrays.toString(configuration.getValueWidths()));

        final FixedWidthDataContext dataContext = new FixedWidthDataContext(dataResource, configuration);

        performAssertionsOnSpec1(dataContext);
    }
    
    @Test
    public void testReadConfigurationFromSasInputMetadata() throws Exception {
        final FixedWidthConfigurationReader reader = new FixedWidthConfigurationReader();
        final Resource resource = new FileResource("src/test/resources/metadata_spec1/sas-input-metadata.txt");
        assertTrue(resource.isExists());

        final FixedWidthConfiguration configuration = reader.readFromSasInputDefinition("UTF8", resource, false);
        assertEquals("[1, 20, 2]", Arrays.toString(configuration.getValueWidths()));

        final FixedWidthDataContext dataContext = new FixedWidthDataContext(dataResource, configuration);

        performAssertionsOnSpec1(dataContext);
    }

    /**
     * Shared assertions section once the 'metadata_spec1' {@link DataContext}
     * has been loaded.
     * 
     * @param dataContext
     */
    private void performAssertionsOnSpec1(FixedWidthDataContext dataContext) {
        final Table table = dataContext.getDefaultSchema().getTable(0);
        final String[] columnNames = table.getColumnNames().toArray(new String[0]);
        assertEquals("[Record type, Description, Initials]", Arrays.toString(columnNames));

        try (final DataSet dataSet = dataContext.query().from(table).selectAll().execute()) {
            assertTrue(dataSet.next());
            assertEquals("Row[values=[P, Kasper Sorensen, KS]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[C, Human Inference, HI]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[P, Ankit Kumar, AK]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[C, Stratio, S]]", dataSet.getRow().toString());
            assertTrue(dataSet.next());
            assertEquals("Row[values=[U, Unknown, ]]", dataSet.getRow().toString());
            assertFalse(dataSet.next());
        }
    }
}

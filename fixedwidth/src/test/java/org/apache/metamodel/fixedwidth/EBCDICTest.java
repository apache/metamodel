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

import java.io.File;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.naming.CustomColumnNamingStrategy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EBCDICTest {
    private static final int[] COLUMN_WIDTHS = new int[] { 2, 7, 10, 10 };
    private static final long EXPECTED_ROWS_COUNT = 49; // 50 lines, 1. is a header
    private static final String ENCODING = "IBM500";
    private static final String[] EXPECTED_ROWS = new String[] {
            "Row[values=[01, name-01, surname-01, address-01]]",
            "Row[values=[02, name-02, surname-02, address-02]]",
            "Row[values=[03, name-03, surname-03, address-03]]",
    };
    private final FixedWidthDataContext _context;
    private final Table _table;

    public EBCDICTest() {
        String fileName = "fixed-width-2-7-10-10.ebc";
        FixedWidthConfiguration configuration = new EbcdicConfiguration(FixedWidthConfiguration.NO_COLUMN_NAME_LINE,
                ENCODING, COLUMN_WIDTHS, false, true, false);
        _context = new FixedWidthDataContext(new File("src/test/resources/" + fileName), configuration);
        Schema schema = _context.getDefaultSchema();
        _table = schema.getTableByName(fileName);
    }

    @Test
    public void testRowsCount() throws Exception {
        long rows = 0;

        try (final DataSet dataSet = _context.query().from(_table).selectCount().execute()) {
            if (dataSet.next()) {
                Object[] values = dataSet.getRow().getValues();
                rows = (long) values[0];
            }
        }

        assertEquals(EXPECTED_ROWS_COUNT, rows);
    }

    @Test
    public void testFirstRows() throws Exception {
        int limit = EXPECTED_ROWS.length;
        int i = 0;

        try (final DataSet dataSet = _context.query().from(_table).selectAll().limit(limit).execute()) {
            while (dataSet.next()) {
                assertEquals(EXPECTED_ROWS[i], dataSet.getRow().toString());
                i++;
            }
        }
    }

    @Test
    public void testCustomColumnNames() throws Exception {
        final String[] columnNames = {"first", "second", "third", "fourth"};
        final FixedWidthConfiguration configuration = new EbcdicConfiguration(
                FixedWidthConfiguration.NO_COLUMN_NAME_LINE, new CustomColumnNamingStrategy(columnNames), ENCODING,
                COLUMN_WIDTHS, false, true, false);
        final DataContext dataContext = new FixedWidthDataContext(new File(
                "src/test/resources/fixed-width-2-7-10-10.ebc"), configuration);
        final Table table = dataContext.getDefaultSchema().getTable(0);

        for (int i = 0; i < columnNames.length; i++) {
            assertNotNull(table.getColumnByName(columnNames[i]));
        }
    }
}

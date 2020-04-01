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

import java.io.File;
import java.text.DateFormat;
import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;

import junit.framework.TestCase;
import org.apache.metamodel.schema.typing.CustomColumnTypingStrategy;
import org.apache.metamodel.util.DateUtils;
import org.apache.metamodel.util.FileHelper;

public class SingleLineCsvDataSetTest extends TestCase {

    public void testGetValueInNonPhysicalOrder() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, true, false);
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_people.csv"), configuration);

        DataSet dataSet = dc.query().from("csv_people.csv").select("age", "name").execute();

        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, mike]]", dataSet.getRow().toString());

        assertEquals("mike", dataSet.getRow().getValue(dc.getColumnByQualifiedLabel("name")));

        assertTrue(dataSet.next());
        assertEquals("Row[values=[19, michael]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[18, peter]]", dataSet.getRow().toString());
        assertTrue(dataSet.next());
        assertEquals("Row[values=[17, bob]]", dataSet.getRow().toString());

        dataSet.close();
    }

    public void testMalformedLineParsing() throws Exception {
        CsvConfiguration configuration = new CsvConfiguration(1, false, false);
        CsvDataContext dc = new CsvDataContext(new File("src/test/resources/csv_malformed_line.txt"), configuration);

        Table table = dc.getDefaultSchema().getTable(0);
        DataSet ds = dc.query().from(table).selectAll().execute();
        assertTrue(ds.next());
        assertEquals("[foo, bar, baz]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[\", null, null]", Arrays.toString(ds.getRow().getValues()));
        assertTrue(ds.next());
        assertEquals("[hello, there, world]", Arrays.toString(ds.getRow().getValues()));
        assertFalse(ds.next());
        ds.close();
    }

    public void testCustomTyping() throws Exception {
        ColumnType columnType1 = ColumnType.DATE;
        ColumnType columnType2 = ColumnType.TIME;
        ColumnType columnType3 = ColumnType.STRING;
        ColumnType columnType4 = ColumnType.NUMBER;
        ColumnType columnType5 = ColumnType.BOOLEAN;

        final CsvConfiguration configuration = new CsvConfiguration( CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, null,
                new CustomColumnTypingStrategy( columnType1, columnType2, columnType3, columnType4, columnType5 ),
                FileHelper.DEFAULT_ENCODING, CsvConfiguration.DEFAULT_SEPARATOR_CHAR,
                CsvConfiguration.DEFAULT_QUOTE_CHAR, CsvConfiguration.DEFAULT_ESCAPE_CHAR, false, true );

        final DataContext dc = new CsvDataContext( new File( "src/test/resources/csv_various_types.csv" ), configuration );
        final Table table = dc.getDefaultSchema().getTable(0);

        DateFormat dateFormat = DateUtils.createDateFormat( "yyyy-MM-dd" );
        DateFormat timeFormat = DateUtils.createDateFormat( "HH:mm" );

        DataSet dataSet = dc.query().from(table).selectAll().execute();

        assertTrue(dataSet.next());
        assertEquals(dateFormat.parse("2008-11-04"), dataSet.getRow().getValues()[0]);
        assertEquals(timeFormat.parse("12:00"), dataSet.getRow().getValues()[1]);
        assertEquals("election day", dataSet.getRow().getValues()[2]);
        assertEquals(8.8, (Double) dataSet.getRow().getValues()[3], Math.ulp( 8.8 ));
        assertFalse((Boolean) dataSet.getRow().getValues()[4]);

        assertTrue(dataSet.next());
        assertEquals(dateFormat.parse("2008-12-24"), dataSet.getRow().getValues()[0]);
        assertEquals(timeFormat.parse("00:00"), dataSet.getRow().getValues()[1]);
        assertEquals("christmas day", dataSet.getRow().getValues()[2]);
        assertEquals(9.0, (Double) dataSet.getRow().getValues()[3], Math.ulp( 9.0 ));
        assertTrue((Boolean) dataSet.getRow().getValues()[4]);

        assertTrue(dataSet.next());
        assertEquals(dateFormat.parse( "2007-12-31"), dataSet.getRow().getValues()[0]);
        assertEquals(timeFormat.parse( "23:59"), dataSet.getRow().getValues()[1]);
        assertEquals("new years eve", dataSet.getRow().getValues()[2]);
        assertEquals(6.4, (Double) dataSet.getRow().getValues()[3], Math.ulp( 6.4 ));
        assertNull(dataSet.getRow().getValues()[4]);
    }


    public void testTypeBasedFiltering() throws Exception {
        File dataFile = new File("src/test/resources/csv_various_types.csv");

        ColumnType columnType1 = ColumnType.DATE;
        ColumnType columnType2 = ColumnType.TIME;
        ColumnType columnType3 = ColumnType.STRING;
        ColumnType columnType4 = ColumnType.NUMBER;
        ColumnType columnType5 = ColumnType.BOOLEAN;

        final CsvConfiguration typedConfiguration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE,
                null, new CustomColumnTypingStrategy(columnType1, columnType2, columnType3, columnType4, columnType5)
                , FileHelper.DEFAULT_ENCODING, CsvConfiguration.DEFAULT_SEPARATOR_CHAR,
                CsvConfiguration.DEFAULT_QUOTE_CHAR, CsvConfiguration.DEFAULT_ESCAPE_CHAR, false, true);
        final DataContext typedDc = new CsvDataContext(dataFile,
                typedConfiguration);
        final Table typedTable = typedDc.getDefaultSchema().getTable(0);

        DataSet typedDataSet =
                typedDc.query().from(typedTable).selectCount().where("rating").greaterThan(10.0).execute();
        assertTrue(typedDataSet.next());
        assertEquals(0L, typedDataSet.getRow().getValue(0));

        // Same query and data set as above, but force toString based comparisons of values for a different result.
        final CsvConfiguration untypedConfiguration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE,
                null, null, FileHelper.DEFAULT_ENCODING, CsvConfiguration.DEFAULT_SEPARATOR_CHAR,
                CsvConfiguration.DEFAULT_QUOTE_CHAR, CsvConfiguration.DEFAULT_ESCAPE_CHAR, false, true);

        final DataContext untypedDataContext =
                new CsvDataContext(dataFile, untypedConfiguration);
        final Table untypedTable = untypedDataContext.getDefaultSchema().getTable(0);

        DataSet untypedDataSet =
                untypedDataContext.query().from(untypedTable).selectCount().where("rating").greaterThan(10.0).execute();
        assertTrue(untypedDataSet.next());
        assertEquals(3L, untypedDataSet.getRow().getValue(0));
    }
}

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.junit.Assert;
import org.junit.Test;

import com.opencsv.CSVParser;

public class SingleLineCsvRowTest {

    @Test
    public void testSerialize() throws Exception {
        final List<Column> columns = new ArrayList<>();
        columns.add(new MutableColumn("1"));
        columns.add(new MutableColumn("2"));
        CSVParser csvParser = new CSVParser();
        final SingleLineCsvDataSet dataSet = new SingleLineCsvDataSet(null, csvParser, columns, null, 2, false);
        final SingleLineCsvRow originalRow = new SingleLineCsvRow(dataSet, "foo,bar", 2, false, 1);

        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(originalRow);
        out.flush();
        bytes.flush();

        final byte[] byteArray = bytes.toByteArray();
        Assert.assertTrue(byteArray.length > 0);
        
        final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        final Row deserializedRow = (Row) in.readObject();

        final Object[] values1 = originalRow.getValues();
        final Object[] values2 = deserializedRow.getValues();
        Assert.assertArrayEquals(values1, values2);
    }
}

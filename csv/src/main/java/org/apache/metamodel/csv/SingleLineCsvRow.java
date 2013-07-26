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

import org.apache.metamodel.data.AbstractRow;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.LazyRef;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Specialized row implementation for single-line CSV values
 */
final class SingleLineCsvRow extends AbstractRow {

    private static final long serialVersionUID = 1L;

    private final SingleLineCsvDataSet _dataSet;
    private final LazyRef<String[]> _valuesRef;

    public SingleLineCsvRow(SingleLineCsvDataSet dataSet, final String line, final int columnsInTable,
            final boolean failOnInconsistentRowLength, final int rowNumber) {
        _dataSet = dataSet;
        _valuesRef = new LazyRef<String[]>() {
            @Override
            protected String[] fetch() throws Throwable {
                final CSVParser parser = _dataSet.getCsvParser();
                final String[] csvValues = parser.parseLine(line);

                if (failOnInconsistentRowLength) {
                    if (columnsInTable != csvValues.length) {
                        throw new InconsistentRowLengthException(columnsInTable, SingleLineCsvRow.this, csvValues,
                                rowNumber);
                    }
                }

                // convert the line's values into the row values that where
                // requested
                final DataSetHeader header = _dataSet.getHeader();
                final int size = header.size();
                final String[] rowValues = new String[size];

                for (int i = 0; i < size; i++) {
                    final Column column = header.getSelectItem(i).getColumn();
                    final int columnNumber = column.getColumnNumber();
                    if (columnNumber < csvValues.length) {
                        rowValues[i] = csvValues[columnNumber];
                    } else {
                        // Ticket #125: Missing values should be enterpreted as
                        // null.
                        rowValues[i] = null;
                    }
                }

                return rowValues;
            }
        };
    }

    @Override
    public Object getValue(int index) throws IndexOutOfBoundsException {
        String[] values = _valuesRef.get();
        if (values == null) {
            return null;
        }
        return values[index];
    }

    @Override
    public Style getStyle(int index) throws IndexOutOfBoundsException {
        return Style.NO_STYLE;
    }

    @Override
    protected DataSetHeader getHeader() {
        return _dataSet.getHeader();
    }

}

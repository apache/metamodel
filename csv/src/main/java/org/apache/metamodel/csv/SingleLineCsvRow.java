/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.csv;

import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.AbstractRow;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Style;
import org.apache.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specialized row implementation for single-line CSV values
 */
final class SingleLineCsvRow extends AbstractRow {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(SingleLineCsvRow.class);

    private final transient SingleLineCsvDataSet _dataSet;

    private final String _line;
    private final int _columnsInTable;
    private final boolean _failOnInconsistentRowLength;
    private final int _rowNumber;
    private final DataSetHeader _header;
    private String[] _values;

    public SingleLineCsvRow(SingleLineCsvDataSet dataSet, final String line, final int columnsInTable,
            final boolean failOnInconsistentRowLength, final int rowNumber) {
        _dataSet = dataSet;
        _header = dataSet.getHeader();
        _line = line;
        _columnsInTable = columnsInTable;
        _failOnInconsistentRowLength = failOnInconsistentRowLength;
        _rowNumber = rowNumber;
        _values = null;
    }

    private String[] getValuesInternal() {
        if (_values == null) {
            final String[] csvValues = parseLine();

            if (_failOnInconsistentRowLength) {
                if (_columnsInTable != csvValues.length) {
                    throw new InconsistentRowLengthException(_columnsInTable, SingleLineCsvRow.this, csvValues,
                            _rowNumber);
                }
            }

            // convert the line's values into the row values that where
            // requested
            final int size = _header.size();
            final String[] rowValues = new String[size];

            for (int i = 0; i < size; i++) {
                final Column column = _header.getSelectItem(i).getColumn();
                final int columnNumber = column.getColumnNumber();
                if (columnNumber < csvValues.length) {
                    rowValues[i] = csvValues[columnNumber];
                } else {
                    // Ticket #125: Missing values should be interpreted as
                    // null.
                    rowValues[i] = null;
                }
            }

            _values = rowValues;
        }
        return _values;
    }

    private void writeObject(ObjectOutputStream stream) throws IOException {
        // ensure that values are loaded
        getValues();
        stream.defaultWriteObject();
    }

    private String[] parseLine() {
        try {
            return _dataSet.getCsvParser().parseLine(_line);
        } catch (IOException e) {
            if (_failOnInconsistentRowLength) {
                throw new MetaModelException("Failed to parse CSV line no. " + _rowNumber + ": " + _line, e);
            } else {
                logger.warn(
                        "Encountered unparseable line no. {}, returning line as a single value with trailing nulls: {}",
                        _rowNumber, _line);
                String[] csvValues = new String[_columnsInTable];
                csvValues[0] = _line;
                return csvValues;
            }
        }
    }

    @Override
    public Object getValue(int index) throws IndexOutOfBoundsException {
        final String[] values = getValuesInternal();
        assert values != null;
        return values[index];
    }

    @Override
    public Style getStyle(int index) throws IndexOutOfBoundsException {
        return Style.NO_STYLE;
    }

    @Override
    protected DataSetHeader getHeader() {
        return _header;
    }

}

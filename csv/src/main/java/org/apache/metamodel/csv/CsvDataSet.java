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

import java.io.IOException;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.FileHelper;

import com.opencsv.CSVReader;

/**
 * Streaming DataSet implementation for CSV support
 */
final class CsvDataSet extends AbstractDataSet {

    private final CSVReader _reader;
    private final boolean _failOnInconsistentRowLength;
    private final int _columnsInTable;
    private volatile int _rowNumber;
    private volatile Integer _rowsRemaining;
    private volatile Row _row;

    public CsvDataSet(CSVReader reader, Column[] columns, Integer maxRows, int columnsInTable,
            boolean failOnInconsistentRowLength) {
        super(columns);
        _reader = reader;
        _columnsInTable = columnsInTable;
        _failOnInconsistentRowLength = failOnInconsistentRowLength;
        _rowNumber = 0;
        _rowsRemaining = maxRows;
    }

    @Override
    public void close() {
        FileHelper.safeClose(_reader);
        _row = null;
        _rowsRemaining = null;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        // close is always safe to invoke
        close();
    }

    @Override
    public Row getRow() throws MetaModelException {
        return _row;
    }

    @Override
    public boolean next() {
        if (_rowsRemaining != null && _rowsRemaining > 0) {
            _rowsRemaining--;
            return nextInternal();
        } else if (_rowsRemaining == null) {
            return nextInternal();
        } else {
            return false;
        }
    }

    private boolean nextInternal() {
        if (_reader == null) {
            return false;
        }
        final String[] csvValues;
        try {
            csvValues = _reader.readNext();
        } catch (IOException e) {
            throw new IllegalStateException("Exception reading from file", e);
        }
        if (csvValues == null) {
            close();
            return false;
        }

        if (csvValues.length == 1 && "".equals(csvValues[0])) {
            // blank line - move to next line
            return nextInternal();
        }

        final int size = getHeader().size();
        final Object[] rowValues = new Object[size];
        for (int i = 0; i < size; i++) {
            Column column = getHeader().getSelectItem(i).getColumn();
            int columnNumber = column.getColumnNumber();
            if (columnNumber < csvValues.length) {
                rowValues[i] = csvValues[columnNumber];
            } else {
                // Ticket #125: Missing values should be enterpreted as
                // null.
                rowValues[i] = null;
            }
        }
        _row = new DefaultRow(getHeader(), rowValues);

        if (_failOnInconsistentRowLength) {
            _rowNumber++;
            if (_columnsInTable != csvValues.length) {
                throw new InconsistentRowLengthException(_columnsInTable, _row, csvValues, _rowNumber);
            }
        }

        return true;
    }
}
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

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.util.FileHelper;

import com.opencsv.ICSVParser;

/**
 * A specialized DataSet implementation for the CSV module under circumstances
 * where multiline values are disabled. In this case we can use a optimized
 * CSVParser and also lazy evaluate lines read from the file.
 */
final class SingleLineCsvDataSet extends AbstractDataSet {

    private final BufferedReader _reader;
    private final ICSVParser _csvParser;
    private final int _columnsInTable;
    private final boolean _failOnInconsistentRowLength;

    private volatile int _rowNumber;
    private volatile Integer _rowsRemaining;
    private volatile Row _row;

    public SingleLineCsvDataSet(BufferedReader reader, ICSVParser csvParser, Column[] columns, Integer maxRows,
            int columnsInTable, boolean failOnInconsistentRowLength) {
        super(columns);
        _reader = reader;
        _csvParser = csvParser;
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

    @Override
    protected DataSetHeader getHeader() {
        // re-make this method protected so that it's visible for
        // SingleLineCsvRow.
        return super.getHeader();
    }

    protected boolean isFailOnInconsistentRowLength() {
        return _failOnInconsistentRowLength;
    }

    protected int getColumnsInTable() {
        return _columnsInTable;
    }

    protected ICSVParser getCsvParser() {
        return _csvParser;
    }

    public boolean nextInternal() {
        if (_reader == null) {
            return false;
        }

        try {
            final String line = _reader.readLine();
            if (line == null) {
                close();
                return false;
            }

            if ("".equals(line)) {
                // blank line - move to next line
                return nextInternal();
            }

            _rowNumber++;
            _row = new SingleLineCsvRow(this, line, _columnsInTable, _failOnInconsistentRowLength, _rowNumber);
            return true;
        } catch (IOException e) {
            close();
            throw new MetaModelException("IOException occurred while reading next line of CSV resource", e);
        }
    }

    @Override
    public Row getRow() {
        return _row;
    }

}

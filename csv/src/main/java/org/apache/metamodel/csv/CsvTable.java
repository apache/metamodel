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

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.AlphabeticSequence;
import org.apache.metamodel.util.FileHelper;

import au.com.bytecode.opencsv.CSVReader;

final class CsvTable extends AbstractTable {

    private static final long serialVersionUID = 1L;

    private final CsvSchema _schema;
    private Column[] _columns;

    /**
     * Constructor for creating a new CSV table which has not yet been written.
     * 
     * @param schema
     * @param columnNames
     */
    public CsvTable(CsvSchema schema, String[] columnNames) {
        this(schema);
        _columns = buildColumns(columnNames);
    }

    /**
     * Constructor for reading an existing CSV table.
     * 
     * @param schema
     */
    public CsvTable(CsvSchema schema) {
        _schema = schema;
    }

    @Override
    public String getName() {
        String schemaName = _schema.getName();
        return schemaName.substring(0, schemaName.length() - 4);
    }

    @Override
    public Column[] getColumns() {
        if (_columns == null) {
            synchronized (this) {
                if (_columns == null) {
                    _columns = buildColumns();
                }
            }
        }
        return _columns;
    }

    private Column[] buildColumns() {
        CSVReader reader = null;
        try {
            reader = _schema.getDataContext().createCsvReader(0);

            final int columnNameLineNumber = _schema.getDataContext().getConfiguration().getColumnNameLineNumber();
            for (int i = 1; i < columnNameLineNumber; i++) {
                reader.readNext();
            }
            final String[] columnHeaders = reader.readNext();

            reader.close();
            return buildColumns(columnHeaders);
        } catch (IOException e) {
            throw new IllegalStateException("Exception reading from resource: " + _schema.getDataContext().getResource().getName(), e);
        } finally {
            FileHelper.safeClose(reader);
        }
    }

    private Column[] buildColumns(final String[] columnNames) {
        if (columnNames == null) {
            return new Column[0];
        }

        final CsvConfiguration configuration = _schema.getDataContext().getConfiguration();
        final int columnNameLineNumber = configuration.getColumnNameLineNumber();
        final boolean nullable = !configuration.isFailOnInconsistentRowLength();

        final Column[] columns = new Column[columnNames.length];
        final AlphabeticSequence sequence = new AlphabeticSequence();
        for (int i = 0; i < columnNames.length; i++) {
            final String columnName;
            if (columnNameLineNumber == CsvConfiguration.NO_COLUMN_NAME_LINE) {
                columnName = sequence.next();
            } else {
                columnName = columnNames[i];
            }
            Column column = new MutableColumn(columnName, ColumnType.VARCHAR, this, i, null, null, nullable, null,
                    false, null);
            columns[i] = column;
        }
        return columns;
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    @Override
    public TableType getType() {
        return TableType.TABLE;
    }

    @Override
    public Relationship[] getRelationships() {
        return new Relationship[0];
    }

    @Override
    public String getRemarks() {
        return null;
    }

    @Override
    public String getQuote() {
        return null;
    }
}

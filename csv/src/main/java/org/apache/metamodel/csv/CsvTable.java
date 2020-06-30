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
import java.io.ObjectInputStream;
import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.schema.naming.ColumnNamingContextImpl;
import org.apache.metamodel.schema.naming.ColumnNamingSession;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.LegacyDeserializationObjectInputStream;

import com.opencsv.CSVReader;

final class CsvTable extends AbstractTable {

    private static final long serialVersionUID = 1L;

    private final CsvSchema _schema;
    private final String _tableName;
    private List<Column> _columns;

    /**
     * Constructor for creating a new CSV table which has not yet been written.
     * 
     * @param schema
     * @param columnNames
     */
    public CsvTable(CsvSchema schema, String tableName, List<String> columnNames) {
        this(schema, tableName);
        _columns = buildColumns(columnNames);
    }

    /**
     * Constructor for reading an existing CSV table.
     * 
     * @param schema
     */
    public CsvTable(CsvSchema schema, String tableName) {
        _schema = schema;
        _tableName = tableName;
    }

    @Override
    public String getName() {
        if (_tableName == null) {
            // can only occur when deserializing legacy objects. Using the
            // legacy MetaModel code for creating table name here.
            String schemaName = _schema.getName();
            return schemaName.substring(0, schemaName.length() - 4);
        }
        return _tableName;
    }

    @Override
    public List<Column> getColumns() {
        if (_columns == null) {
            synchronized (this) {
                if (_columns == null) {
                    _columns = buildColumns();
                }
            }
        }
        return _columns;
    }

    private List<Column> buildColumns() {
        CSVReader reader = null;
        try {
            reader = _schema.getDataContext().createCsvReader(0);

            final int columnNameLineNumber = _schema.getDataContext().getConfiguration().getColumnNameLineNumber();
            for (int i = 1; i < columnNameLineNumber; i++) {
                reader.readNext();
            }
            final List<String> columnHeaders = Arrays.asList(Optional.ofNullable(reader.readNext()).orElse(new String[0]));

            reader.close();
            return buildColumns(columnHeaders);
        } catch (IOException e) {
            throw new IllegalStateException("Exception reading from resource: "
                    + _schema.getDataContext().getResource().getName(), e);
        } finally {
            FileHelper.safeClose(reader);
        }
    }

    private List<Column> buildColumns(final List<String> columnNames) {
        if (columnNames == null) {
            return new ArrayList<>();
        }
        
        final CsvConfiguration configuration = _schema.getDataContext().getConfiguration();
        final int columnNameLineNumber = configuration.getColumnNameLineNumber();
        final boolean nullable = !configuration.isFailOnInconsistentRowLength();
        final ColumnNamingStrategy columnNamingStrategy = configuration.getColumnNamingStrategy();

        List<Column> columns = new ArrayList<>();

        try (final ColumnNamingSession namingSession = columnNamingStrategy.startColumnNamingSession()) {
            for (int i = 0; i < columnNames.size(); i++) {
                final String intrinsicColumnName = columnNameLineNumber == CsvConfiguration.NO_COLUMN_NAME_LINE ? null
                        : columnNames.get(i);
                final String columnName = namingSession.getNextColumnName(new ColumnNamingContextImpl(this,
                        intrinsicColumnName, i));
                final Column column = new MutableColumn(columnName, ColumnType.STRING, this, i, null, null, nullable,
                        null, false, null);
                columns.add(column);
            }
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
    public Collection<Relationship> getRelationships() {
        return Collections.emptyList();
    }

    @Override
    public String getRemarks() {
        return null;
    }

    @Override
    public String getQuote() {
        return null;
    }

    private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        final GetField getFields = stream.readFields();
        Object columns = getFields.get("_columns", null);
        if (columns instanceof Column[]) {
            columns = Arrays.<Column> asList((Column[]) columns);
        }
        final Object schema = getFields.get("_schema", null);
        final Object tableName = getFields.get("_tableName", null);
        LegacyDeserializationObjectInputStream.setField(CsvTable.class, this, "_columns", columns);
        LegacyDeserializationObjectInputStream.setField(CsvTable.class, this, "_schema", schema);
        LegacyDeserializationObjectInputStream.setField(CsvTable.class, this, "_tableName", tableName);
    }
}

/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.csv;

import java.io.IOException;

import org.eobjects.metamodel.schema.AbstractTable;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.Relationship;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.util.AlphabeticSequence;
import org.eobjects.metamodel.util.FileHelper;

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

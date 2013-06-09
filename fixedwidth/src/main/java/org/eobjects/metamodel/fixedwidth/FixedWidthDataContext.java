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
package org.eobjects.metamodel.fixedwidth;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.util.AlphabeticSequence;
import org.eobjects.metamodel.util.FileHelper;
import org.eobjects.metamodel.util.FileResource;
import org.eobjects.metamodel.util.Resource;

/**
 * DataContext implementation for fixed width value files.
 * 
 * @author Kasper Sørensen
 */
public class FixedWidthDataContext extends QueryPostprocessDataContext {

    private final Resource _resource;
    private final FixedWidthConfiguration _configuration;

    /**
     * @deprecated use
     *             {@link #FixedWidthDataContext(File, FixedWidthConfiguration)}
     *             instead
     */
    @Deprecated
    public FixedWidthDataContext(String filename, String fileEncoding, int fixedValueWidth) {
        this(new FileResource(filename), new FixedWidthConfiguration(fixedValueWidth));
    }

    /**
     * @deprecated use
     *             {@link #FixedWidthDataContext(File, FixedWidthConfiguration)}
     *             instead
     */
    @Deprecated
    public FixedWidthDataContext(File file, String fileEncoding, int fixedValueWidth, int headerLineNumber) {
        this(file, new FixedWidthConfiguration(headerLineNumber, fileEncoding, fixedValueWidth));
    }

    public FixedWidthDataContext(File file, FixedWidthConfiguration configuration) {
        _resource = new FileResource(file);
        _configuration = configuration;
    }

    public FixedWidthDataContext(Resource resource, FixedWidthConfiguration configuration) {
        _resource = resource;
        _configuration = configuration;
    }

    /**
     * Gets the Fixed width value configuration used.
     * 
     * @return a fixed width configuration
     */
    public FixedWidthConfiguration getConfiguration() {
        return _configuration;
    }

    /**
     * Gets the file being read.
     * 
     * @return a file
     * 
     * @deprecated use {@link #getResource()} instead.
     */
    @Deprecated
    public File getFile() {
        if (_resource instanceof FileResource) {
            return ((FileResource) _resource).getFile();
        }
        return null;
    }

    /**
     * Gets the resource being read
     * 
     * @return
     */
    public Resource getResource() {
        return _resource;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final String schemaName = getDefaultSchemaName();
        final MutableSchema schema = new MutableSchema(schemaName);
        final String tableName = schemaName.substring(0, schemaName.length() - 4);
        final MutableTable table = new MutableTable(tableName, TableType.TABLE, schema);
        schema.addTable(table);

        final FixedWidthReader reader = createReader();
        final String[] columnNames;
        try {
            if (_configuration.getColumnNameLineNumber() != FixedWidthConfiguration.NO_COLUMN_NAME_LINE) {
                for (int i = 1; i < _configuration.getColumnNameLineNumber(); i++) {
                    reader.readLine();
                }
                columnNames = reader.readLine();
            } else {
                columnNames = reader.readLine();
                if (columnNames != null) {
                    AlphabeticSequence sequence = new AlphabeticSequence();
                    for (int i = 0; i < columnNames.length; i++) {
                        columnNames[i] = sequence.next();
                    }
                }
            }
        } finally {
            FileHelper.safeClose(reader);
        }

        if (columnNames != null) {
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                final MutableColumn column = new MutableColumn(columnName, ColumnType.VARCHAR, table, i, true);
                column.setColumnSize(_configuration.getValueWidth(i));
                table.addColumn(column);
            }
        }

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _resource.getName();
    }

    @Override
    public DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final FixedWidthReader reader = createReader();
        try {
            for (int i = 1; i <= _configuration.getColumnNameLineNumber(); i++) {
                reader.readLine();
            }
        } catch (IllegalStateException e) {
            FileHelper.safeClose(reader);
            throw e;
        }
        if (maxRows > 0) {
            return new FixedWidthDataSet(reader, columns, maxRows);
        } else {
            return new FixedWidthDataSet(reader, columns, null);
        }
    }

    private FixedWidthReader createReader() {
        final InputStream inputStream = _resource.read();
        final Reader fileReader = FileHelper.getReader(inputStream, _configuration.getEncoding());
        final FixedWidthReader reader;
        if (_configuration.isConstantValueWidth()) {
            reader = new FixedWidthReader(fileReader, _configuration.getFixedValueWidth(),
                    _configuration.isFailOnInconsistentLineWidth());
        } else {
            reader = new FixedWidthReader(fileReader, _configuration.getValueWidths(),
                    _configuration.isFailOnInconsistentLineWidth());
        }
        return reader;
    }

}

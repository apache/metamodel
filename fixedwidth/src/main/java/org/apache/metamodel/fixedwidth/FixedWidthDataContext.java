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
package org.apache.metamodel.fixedwidth;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.InputStream;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.schema.naming.ColumnNamingContextImpl;
import org.apache.metamodel.schema.naming.ColumnNamingSession;
import org.apache.metamodel.schema.naming.ColumnNamingStrategy;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.ResourceUtils;

/**
 * DataContext implementation for fixed width value files.
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
     * @return a {@link Resource} object
     */
    public Resource getResource() {
        return _resource;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final String schemaName = getDefaultSchemaName();
        final MutableSchema schema = new MutableSchema(schemaName);
        final String tableName = _resource.getName();
        final MutableTable table = new MutableTable(tableName, TableType.TABLE, schema);
        schema.addTable(table);

        final FixedWidthReader reader = createReader();
        final String[] columnNames;
        try {
            final boolean hasColumnHeader = _configuration
                    .getColumnNameLineNumber() != FixedWidthConfiguration.NO_COLUMN_NAME_LINE;
            if (hasColumnHeader) {
                for (int i = 1; i < _configuration.getColumnNameLineNumber(); i++) {
                    reader.readLine();
                }
                columnNames = reader.readLine();
            } else {
                columnNames = reader.readLine();
            }
            final ColumnNamingStrategy columnNamingStrategy = _configuration.getColumnNamingStrategy();
            if (columnNames != null) {
                try (final ColumnNamingSession columnNamingSession = columnNamingStrategy.startColumnNamingSession()) {
                    for (int i = 0; i < columnNames.length; i++) {
                        final String intrinsicColumnName = hasColumnHeader ? columnNames[i] : null;
                        columnNames[i] = columnNamingSession.getNextColumnName(new ColumnNamingContextImpl(table,
                                intrinsicColumnName, i));
                    }
                }
            }
        } finally {
            FileHelper.safeClose(reader);
        }

        if (columnNames != null) {
            for (int i = 0; i < columnNames.length; i++) {
                final String columnName = columnNames[i];
                final MutableColumn column = new MutableColumn(columnName, ColumnType.STRING, table, i, true);
                column.setColumnSize(_configuration.getValueWidth(i));
                table.addColumn(column);
            }
        }

        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return ResourceUtils.getParentName(_resource);
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
        final FixedWidthReader reader;
        
        if (_configuration instanceof EbcdicConfiguration) {
            reader = new EbcdicReader((BufferedInputStream) inputStream, _configuration.getEncoding(),
                    _configuration.getValueWidths(), _configuration.isFailOnInconsistentLineWidth(), 
                    ((EbcdicConfiguration) _configuration).isSkipEbcdicHeader(), 
                    ((EbcdicConfiguration) _configuration).isEolPresent());
        } else {
            if (_configuration.isConstantValueWidth()) {
                reader = new FixedWidthReader(inputStream, _configuration.getEncoding(),
                        _configuration.getFixedValueWidth(), _configuration.isFailOnInconsistentLineWidth());
            } else {
                reader = new FixedWidthReader(inputStream, _configuration.getEncoding(), 
                        _configuration.getValueWidths(), _configuration.isFailOnInconsistentLineWidth());
            }
        }

        return reader;
    }
}

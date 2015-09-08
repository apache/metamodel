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
package org.apache.metamodel.excel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import org.apache.poi.POIXMLDocument;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.LazyRef;
import org.apache.metamodel.util.Ref;
import org.apache.metamodel.util.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link DataContext} implementation to use for Excel spreadsheets.
 * 
 * This DataContext supports both the "old" .xls format and the "new" .xlsx
 * format, and saves the user the trouble of figuring out which one to use,
 * simply by detecting it at runtime and delegating to the appropriate
 * implementation.
 */
public final class ExcelDataContext extends QueryPostprocessDataContext implements UpdateableDataContext {

    private static final Logger logger = LoggerFactory.getLogger(ExcelDataContext.class);

    private final Object WRITE_LOCK = new Object();

    private final Resource _resource;
    private final ExcelConfiguration _configuration;
    private SpreadsheetReaderDelegate _spreadsheetReaderDelegate;

    /**
     * Constructs an Excel DataContext based on a file, with default
     * configuration
     * 
     * @param file
     */
    public ExcelDataContext(File file) {
        this(file, new ExcelConfiguration());
    }

    /**
     * Constructs an Excel DataContext based on a resource and a custom
     * configuration.
     *
     * The file provided can be either existing or non-existing. In the case of
     * non-existing files, a file will be automatically created when a CREATE
     * TABLE update is executed on the DataContext.
     * 
     * @param file
     * @param configuration
     */
    public ExcelDataContext(File file, ExcelConfiguration configuration) {
        if (file == null) {
            throw new IllegalArgumentException("File cannot be null");
        }
        if (configuration == null) {
            throw new IllegalArgumentException("ExcelConfiguration cannot be null");
        }
        if (file.exists() && !file.canRead()) {
            throw new IllegalArgumentException("Cannot read from file");
        }
        _resource = new FileResource(file);
        _configuration = configuration;
    }

    public ExcelDataContext(Resource resource, ExcelConfiguration configuration) {
        if (resource == null) {
            throw new IllegalArgumentException("Resource cannot be null");
        }
        if (configuration == null) {
            throw new IllegalArgumentException("ExcelConfiguration cannot be null");
        }
        _resource = resource;
        _configuration = configuration;
    }

    /**
     * Gets the Excel configuration used.
     * 
     * @return an excel configuration.
     */
    public ExcelConfiguration getConfiguration() {
        return _configuration;
    }

    /**
     * Gets the Excel file being read.
     * 
     * @return a file.
     * @deprecated
     */
    @Deprecated
    public File getFile() {
        if (_resource instanceof FileResource) {
            return ((FileResource) _resource).getFile();
        }
        return null;
    }

    /**
     * Gets the Excel resource being read
     * 
     * @return
     */
    public Resource getResource() {
        return _resource;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _resource.getName();
    }

    @Override
    public DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        try {
            SpreadsheetReaderDelegate delegate = getSpreadsheetReaderDelegate();

            // METAMODEL-47: Ensure that we have loaded the schema at this point
            getDefaultSchema();

            DataSet dataSet = delegate.executeQuery(_resource, table, columns, maxRows);
            return dataSet;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new MetaModelException("Unexpected exception while materializing main schema table", e);
        }
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        if (!_resource.isExists()) {
            logger.info("Resource does not exist, returning empty schema");
            return new MutableSchema(getMainSchemaName());
        }
        Ref<InputStream> inputStreamRef = getInputStreamRef();
        InputStream inputStream = null;
        try {
            SpreadsheetReaderDelegate delegate = getSpreadsheetReaderDelegate();
            inputStream = inputStreamRef.get();
            Schema schema = delegate.createSchema(_resource, getMainSchemaName());
            assert getMainSchemaName().equals(schema.getName());
            return schema;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new MetaModelException("Unexpected exception while building main schema", e);
        } finally {
            FileHelper.safeClose(inputStream);
        }
    }

    @Override
    protected void onSchemaCacheRefreshed() {
        super.onSchemaCacheRefreshed();
        _spreadsheetReaderDelegate = null;
    }

    /**
     * Convenient method for testing and inspecting internal state.
     * 
     * @return the class of the spreadsheet reader delegate.
     */
    protected Class<? extends SpreadsheetReaderDelegate> getSpreadsheetReaderDelegateClass() {
        if (_spreadsheetReaderDelegate != null) {
            return _spreadsheetReaderDelegate.getClass();
        }
        return null;
    }

    private SpreadsheetReaderDelegate getSpreadsheetReaderDelegate()
            throws MetaModelException {
        if (_spreadsheetReaderDelegate == null) {
            synchronized (this) {
                if (_spreadsheetReaderDelegate == null) {
                    try {
                        if (POIXMLDocument.hasOOXMLHeader(getInputStream())) {
                            _spreadsheetReaderDelegate = new XlsxSpreadsheetReaderDelegate(_configuration);
                        } else {
                            _spreadsheetReaderDelegate = new DefaultSpreadsheetReaderDelegate(_configuration);
                        }
                    } catch (IOException e) {
                        logger.error("Could not identify spreadsheet type, using default", e);
                        _spreadsheetReaderDelegate = new DefaultSpreadsheetReaderDelegate(_configuration);
                    }
                }
            }
        }
        return _spreadsheetReaderDelegate;
    }

    private InputStream getInputStream() throws MetaModelException {
        InputStream inputStream = _resource.read();
        if (!inputStream.markSupported()) {
            inputStream = new PushbackInputStream(inputStream, 8);
        }
        return inputStream;
    }

    private LazyRef<InputStream> getInputStreamRef() throws MetaModelException {
        final LazyRef<InputStream> inputStreamRef = new LazyRef<InputStream>() {
            @Override
            public InputStream fetch() {
                InputStream inputStream = getInputStream();
                return inputStream;
            }
        };
        return inputStreamRef;
    }

    protected void notifyTablesModified() {
        LazyRef<InputStream> inputStreamRef = getInputStreamRef();
        try {
            getSpreadsheetReaderDelegate().notifyTablesModified(_resource);
        } finally {
            if (inputStreamRef.isFetched()) {
                FileHelper.safeClose(inputStreamRef.get());
            }
        }
    }

    @Override
    public void executeUpdate(UpdateScript update) {
        ExcelUpdateCallback updateCallback = new ExcelUpdateCallback(this);
        synchronized (WRITE_LOCK) {
            try {
                update.run(updateCallback);
            } finally {
                updateCallback.close();
            }
        }
    }
}
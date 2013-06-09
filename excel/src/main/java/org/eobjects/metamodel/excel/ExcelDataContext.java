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
package org.eobjects.metamodel.excel;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import org.apache.poi.POIXMLDocument;
import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.QueryPostprocessDataContext;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.FileHelper;
import org.eobjects.metamodel.util.FileResource;
import org.eobjects.metamodel.util.LazyRef;
import org.eobjects.metamodel.util.Ref;
import org.eobjects.metamodel.util.Resource;
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

        Ref<InputStream> inputStreamRef = getInputStreamRef();
        InputStream inputStream = null;
        try {
            SpreadsheetReaderDelegate delegate = getSpreadsheetReaderDelegate(inputStreamRef);
            inputStream = inputStreamRef.get();
            DataSet dataSet = delegate.executeQuery(inputStream, table, columns, maxRows);
            return dataSet;
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new MetaModelException("Unexpected exception while materializing main schema table", e);
        } finally {
            FileHelper.safeClose(inputStream);
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
            SpreadsheetReaderDelegate delegate = getSpreadsheetReaderDelegate(inputStreamRef);
            inputStream = inputStreamRef.get();
            Schema schema = delegate.createSchema(inputStream, getMainSchemaName());
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

    private SpreadsheetReaderDelegate getSpreadsheetReaderDelegate(Ref<InputStream> inputStream) throws MetaModelException {
        if (_spreadsheetReaderDelegate == null) {
            synchronized (this) {
                if (_spreadsheetReaderDelegate == null) {
                    try {
                        if (POIXMLDocument.hasOOXMLHeader(inputStream.get())) {
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
            getSpreadsheetReaderDelegate(inputStreamRef).notifyTablesModified(inputStreamRef);
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
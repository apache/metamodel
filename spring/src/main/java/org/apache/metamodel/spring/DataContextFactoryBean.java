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
package org.apache.metamodel.spring;

import javax.sql.DataSource;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.SimpleTableDef;
import org.apache.metamodel.util.SimpleTableDefParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 * A spring {@link FactoryBean} that produces MetaModel {@link DataContext}
 * objects based on property values that will typically be injected by the
 * Spring configuration.
 */
public class DataContextFactoryBean implements FactoryBean<DataContext>, DataContextFactoryParameters {

    private static final Logger logger = LoggerFactory.getLogger(DataContextFactoryBean.class);

    private String _type;
    private org.springframework.core.io.Resource _resource;
    private String _filename;
    private String _url;
    private String _columnNameLineNumber;
    private String _skipEmptyLines;
    private String _skipEmptyColumns;
    private String _encoding;
    private String _separatorChar;
    private String _quoteChar;
    private String _escapeChar;
    private String _failOnInconsistentRowLength;
    private String _multilineValues;
    private TableType[] _tableTypes;
    private String _catalogName;
    private DataSource _dataSource;
    private String _username;
    private String _password;
    private String _driverClassName;
    private String _hostname;
    private Integer _port;
    private String _databaseName;
    private SimpleTableDef[] _tableDefs;

    @Override
    public DataContext getObject() throws Exception {
        final String type = _type == null ? "" : _type.toLowerCase();
        final DataContextFactoryBeanDelegate delegate;

        if (type.isEmpty()) {
            throw new IllegalArgumentException("No DataContext 'type' property provided for DataContextFactoryBean");
        } else if ("csv".equals(type)) {
            delegate = new CsvDataContextFactoryBeanDelegate();
        } else if ("excel".equals(type)) {
            delegate = new ExcelDataContextFactoryBeanDelegate();
        } else if ("jdbc".equals(type)) {
            delegate = new JdbcDataContextFactoryBeanDelegate();
        } else if ("couchdb".equals(type)) {
            delegate = new CouchDbDataContextFactoryBeanDelegate();
        } else if ("mongodb".equals(type)) {
            delegate = new MongoDbDataContextFactoryBeanDelegate();
        } else if ("pojo".equals(type)) {
            delegate = new PojoDataContextFactoryBeanDelegate();
        } else {
            delegate = createDelegateFromType();
        }

        if (delegate == null) {
            throw new UnsupportedOperationException("Unsupported DataContext type: " + _type);
        }

        final DataContext dataContext = delegate.createDataContext(this);
        if (dataContext == null) {
            throw new IllegalStateException("Factory method failed to produce a non-null DataContext instance");
        }

        return dataContext;
    }

    private DataContextFactoryBeanDelegate createDelegateFromType() {
        final Class<?> cls;
        try {
            logger.debug("Attempting to interpret type '{}' as a delegate class name", _type);
            cls = Class.forName(_type.trim());
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to load type '" + _type + "' as a class", e);
            }
            return null;
        }

        if (cls == null) {
            return null;
        }

        if (!DataContextFactoryBeanDelegate.class.isAssignableFrom(cls)) {
            return null;
        }

        try {
            final DataContextFactoryBeanDelegate delegate = cls.asSubclass(DataContextFactoryBeanDelegate.class)
                    .newInstance();
            return delegate;
        } catch (Exception e) {
            logger.warn("Failed to instantiate delegate " + cls, e);
            return null;
        }
    }

    /**
     * Sets the {@link SimpleTableDef}s of {@link #getTableDefs()} by providing
     * a string representation of the following form (like a CREATE TABLE
     * statement, except for the literal 'CREATE TABLE' prefix, and without
     * column sizes):
     * 
     * <code>
     * tablename1 (
     * columnName1 VARCHAR,
     * columnName2 INTEGER,
     * columnName3 DATE
     * );
     * 
     * tablename2 (
     * columnName4 BIGINT,
     * columnName5 CHAR,
     * columnName6 BINARY
     * );
     * </code>
     * 
     * Each table definition is delimited/ended by the semi-colon (;) character.
     * 
     * The parser is at this point quite simple and restricts that column names
     * cannot contain parentheses, commas, semi-colons or spaces. No quote
     * characters or escape characters are available. Newlines, return carriages
     * and tabs are ignored.
     * 
     * @param tableDefinitionsText
     * 
     * @throws IllegalArgumentException
     */
    public void setTableDefinitions(String tableDefinitionsText) throws IllegalArgumentException {
        _tableDefs = SimpleTableDefParser.parseTableDefs(tableDefinitionsText);
    }

    

    @Override
    public Class<DataContext> getObjectType() {
        return DataContext.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    public String getType() {
        return _type;
    }

    public void setType(String type) {
        _type = type;
    }

    @Override
    public org.springframework.core.io.Resource getResource() {
        return _resource;
    }

    public void setResource(org.springframework.core.io.Resource resource) {
        _resource = resource;
    }

    @Override
    public String getFilename() {
        return _filename;
    }

    public void setFilename(String filename) {
        _filename = filename;
    }

    @Override
    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    @Override
    public String getColumnNameLineNumber() {
        return _columnNameLineNumber;
    }

    public void setColumnNameLineNumber(String columnNameLineNumber) {
        _columnNameLineNumber = columnNameLineNumber;
    }

    @Override
    public String getSkipEmptyLines() {
        return _skipEmptyLines;
    }

    public void setSkipEmptyLines(String skipEmptyLines) {
        _skipEmptyLines = skipEmptyLines;
    }

    @Override
    public String getSkipEmptyColumns() {
        return _skipEmptyColumns;
    }

    public void setSkipEmptyColumns(String skipEmptyColumns) {
        _skipEmptyColumns = skipEmptyColumns;
    }

    @Override
    public String getEncoding() {
        return _encoding;
    }

    public void setEncoding(String encoding) {
        _encoding = encoding;
    }

    @Override
    public String getSeparatorChar() {
        return _separatorChar;
    }

    public void setSeparatorChar(String separatorChar) {
        _separatorChar = separatorChar;
    }

    @Override
    public String getQuoteChar() {
        return _quoteChar;
    }

    public void setQuoteChar(String quoteChar) {
        _quoteChar = quoteChar;
    }

    @Override
    public String getEscapeChar() {
        return _escapeChar;
    }

    public void setEscapeChar(String escapeChar) {
        _escapeChar = escapeChar;
    }

    @Override
    public String getFailOnInconsistentRowLength() {
        return _failOnInconsistentRowLength;
    }

    public void setFailOnInconsistentRowLength(String failOnInconsistentRowLength) {
        _failOnInconsistentRowLength = failOnInconsistentRowLength;
    }

    @Override
    public String getMultilineValues() {
        return _multilineValues;
    }

    public void setMultilineValues(String multilineValues) {
        _multilineValues = multilineValues;
    }

    @Override
    public TableType[] getTableTypes() {
        return _tableTypes;
    }

    public void setTableTypes(TableType[] tableTypes) {
        _tableTypes = tableTypes;
    }

    @Override
    public String getCatalogName() {
        return _catalogName;
    }

    public void setCatalogName(String catalogName) {
        _catalogName = catalogName;
    }

    @Override
    public DataSource getDataSource() {
        return _dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        _dataSource = dataSource;
    }

    @Override
    public String getUsername() {
        return _username;
    }

    public void setUsername(String username) {
        _username = username;
    }

    @Override
    public String getPassword() {
        return _password;
    }

    public void setPassword(String password) {
        _password = password;
    }

    @Override
    public String getDriverClassName() {
        return _driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        _driverClassName = driverClassName;
    }

    @Override
    public String getHostname() {
        return _hostname;
    }

    public void setHostname(String hostname) {
        _hostname = hostname;
    }

    @Override
    public Integer getPort() {
        return _port;
    }

    public void setPort(Integer port) {
        _port = port;
    }

    @Override
    public String getDatabaseName() {
        return _databaseName;
    }

    public void setDatabaseName(String databaseName) {
        _databaseName = databaseName;
    }

    @Override
    public SimpleTableDef[] getTableDefs() {
        return _tableDefs;
    }

    public void setTableDefs(SimpleTableDef[] tableDefs) {
        _tableDefs = tableDefs;
    }
}

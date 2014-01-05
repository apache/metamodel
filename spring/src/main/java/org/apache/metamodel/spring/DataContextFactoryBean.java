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

import java.sql.Connection;
import java.sql.DriverManager;

import javax.sql.DataSource;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.excel.ExcelConfiguration;
import org.apache.metamodel.excel.ExcelDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.UrlResource;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.FactoryBean;

/**
 * A spring {@link FactoryBean} that produces MetaModel {@link DataContext}
 * objects based on property values that will typically be injected by the
 * Spring configuration.
 */
public class DataContextFactoryBean implements FactoryBean<DataContext> {

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

    @Override
    public DataContext getObject() throws Exception {
        String type = _type == null ? "" : _type.toLowerCase();
        if ("csv".equals(type)) {
            return createCsvDataContext();
        } else if ("excel".equals(type)) {
            return createExcelDataContext();
        } else if ("jdbc".equals(type)) {
            return createJdbcDataContext();
        }

        // TODO Auto-generated method stub
        return null;
    }

    private DataContext createJdbcDataContext() {
        TableType[] tableTypes = _tableTypes;
        if (tableTypes == null) {
            tableTypes = TableType.DEFAULT_TABLE_TYPES;
        }

        if (_dataSource == null) {
            final String driverClassName = getString(_driverClassName, null);
            if (driverClassName != null) {
                try {
                    Class.forName(driverClassName);
                } catch (ClassNotFoundException e) {
                    logger.error("Failed to initialize JDBC driver class '" + driverClassName + "'!", e);
                }
            }

            final Connection connection;
            try {
                if (_username == null && _password == null) {
                    connection = DriverManager.getConnection(_url);
                } else {
                    connection = DriverManager.getConnection(_url, _username, _password);
                }
            } catch (Exception e) {
                logger.error("Failed to get JDBC connection using URL: " + _url, e);
                throw new IllegalStateException("Failed to get JDBC connection", e);
            }

            return new JdbcDataContext(connection, tableTypes, _catalogName);
        }

        return new JdbcDataContext(_dataSource, tableTypes, _catalogName);
    }

    private DataContext createExcelDataContext() {
        final int columnNameLineNumber = getInt(_columnNameLineNumber, ExcelConfiguration.DEFAULT_COLUMN_NAME_LINE);
        final boolean skipEmptyLines = getBoolean(_skipEmptyLines, true);
        final boolean skipEmptyColumns = getBoolean(_skipEmptyColumns, false);
        final ExcelConfiguration configuration = new ExcelConfiguration(columnNameLineNumber, skipEmptyLines,
                skipEmptyColumns);
        return new ExcelDataContext(getResourceInternal(), configuration);
    }

    private DataContext createCsvDataContext() {
        final int columnNameLineNumber = getInt(_columnNameLineNumber, CsvConfiguration.DEFAULT_COLUMN_NAME_LINE);
        final String encoding = getString(_encoding, FileHelper.DEFAULT_ENCODING);
        final char separatorChar = getChar(_separatorChar, CsvConfiguration.DEFAULT_SEPARATOR_CHAR);
        final char quoteChar = getChar(_quoteChar, CsvConfiguration.DEFAULT_QUOTE_CHAR);
        final char escapeChar = getChar(_escapeChar, CsvConfiguration.DEFAULT_ESCAPE_CHAR);
        final boolean failOnInconsistentRowLength = getBoolean(_failOnInconsistentRowLength, false);
        final boolean multilineValues = getBoolean(_multilineValues, true);
        final CsvConfiguration configuration = new CsvConfiguration(columnNameLineNumber, encoding, separatorChar,
                quoteChar, escapeChar, failOnInconsistentRowLength, multilineValues);
        return new CsvDataContext(getResourceInternal(), configuration);
    }

    private Resource getResourceInternal() {
        if (_resource != null) {
            return new SpringResource(_resource);
        } else if (_filename != null) {
            return new FileResource(_filename);
        } else if (_url != null) {
            return new UrlResource(_url);
        }
        return null;
    }

    @Override
    public Class<DataContext> getObjectType() {
        return DataContext.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    private int getInt(String value, int ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        return Integer.parseInt(value);
    }

    private String getString(String value, String ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        return value;
    }

    private char getChar(String value, char ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        if ("none".equalsIgnoreCase(value)) {
            return CsvConfiguration.NOT_A_CHAR;
        }
        return value.charAt(0);
    }

    private boolean getBoolean(String value, boolean ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        return BooleanComparator.parseBoolean(value);
    }

    public String getType() {
        return _type;
    }

    public void setType(String type) {
        _type = type;
    }

    public org.springframework.core.io.Resource getResource() {
        return _resource;
    }

    public void setResource(org.springframework.core.io.Resource resource) {
        _resource = resource;
    }

    public String getFilename() {
        return _filename;
    }

    public void setFilename(String filename) {
        _filename = filename;
    }

    public String getUrl() {
        return _url;
    }

    public void setUrl(String url) {
        _url = url;
    }

    public String getColumnNameLineNumber() {
        return _columnNameLineNumber;
    }

    public void setColumnNameLineNumber(String columnNameLineNumber) {
        _columnNameLineNumber = columnNameLineNumber;
    }

    public String getSkipEmptyLines() {
        return _skipEmptyLines;
    }

    public void setSkipEmptyLines(String skipEmptyLines) {
        _skipEmptyLines = skipEmptyLines;
    }

    public String getSkipEmptyColumns() {
        return _skipEmptyColumns;
    }

    public void setSkipEmptyColumns(String skipEmptyColumns) {
        _skipEmptyColumns = skipEmptyColumns;
    }

    public String getEncoding() {
        return _encoding;
    }

    public void setEncoding(String encoding) {
        _encoding = encoding;
    }

    public String getSeparatorChar() {
        return _separatorChar;
    }

    public void setSeparatorChar(String separatorChar) {
        _separatorChar = separatorChar;
    }

    public String getQuoteChar() {
        return _quoteChar;
    }

    public void setQuoteChar(String quoteChar) {
        _quoteChar = quoteChar;
    }

    public String getEscapeChar() {
        return _escapeChar;
    }

    public void setEscapeChar(String escapeChar) {
        _escapeChar = escapeChar;
    }

    public String getFailOnInconsistentRowLength() {
        return _failOnInconsistentRowLength;
    }

    public void setFailOnInconsistentRowLength(String failOnInconsistentRowLength) {
        _failOnInconsistentRowLength = failOnInconsistentRowLength;
    }

    public String getMultilineValues() {
        return _multilineValues;
    }

    public void setMultilineValues(String multilineValues) {
        _multilineValues = multilineValues;
    }

    public TableType[] getTableTypes() {
        return _tableTypes;
    }

    public void setTableTypes(TableType[] tableTypes) {
        _tableTypes = tableTypes;
    }

    public String getCatalogName() {
        return _catalogName;
    }

    public void setCatalogName(String catalogName) {
        _catalogName = catalogName;
    }

    public DataSource getDataSource() {
        return _dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        _dataSource = dataSource;
    }

    public String getUsername() {
        return _username;
    }

    public void setUsername(String username) {
        _username = username;
    }

    public String getPassword() {
        return _password;
    }

    public void setPassword(String password) {
        _password = password;
    }

    public String getDriverClassName() {
        return _driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        _driverClassName = driverClassName;
    }

}

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
package org.apache.metamodel.factory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.NumberComparator;
import org.apache.metamodel.util.SimpleTableDef;
import org.apache.metamodel.util.SimpleTableDefParser;

public class DataContextPropertiesImpl implements DataContextProperties {

    private static final long serialVersionUID = 1L;

    public static final String PROPERTY_USERNAME = "username";
    public static final String PROPERTY_PASSWORD = "password";
    public static final String PROPERTY_DRIVER_CLASS = "driver-class";
    public static final String PROPERTY_HOSTNAME = "hostname";
    public static final String PROPERTY_PORT = "port";
    public static final String PROPERTY_DATABASE = "database";
    public static final String PROPERTY_URL = "url";
    public static final String PROPERTY_CATALOG_NAME = "catalog";
    public static final String PROPERTY_RESOURCE_PROPERTIES = "resource";
    public static final String PROPERTY_IS_MULTILINE_VALUES_ENABLED = "multiline-values";
    public static final String PROPERTY_IS_FAIL_ON_INCONSISTENT_ROW_LENGTH = "fail-on-inconsistent-row-length";
    public static final String PROPERTY_ESCAPE_CHAR = "escape-char";
    public static final String PROPERTY_QUOTE_CHAR = "quote-char";
    public static final String PROPERTY_SEPARATOR_CHAR = "separator-char";
    public static final String PROPERTY_ENCODING = "encoding";
    public static final String PROPERTY_SKIP_EMPTY_COLUMNS = "skip-empty-columns";
    public static final String PROPERTY_SKIP_EMPTY_LINES = "skip-empty-lines";
    public static final String PROPERTY_COLUMN_NAME_LINE_NUMBER = "column-name-line-number";
    public static final String PROPERTY_DATA_CONTEXT_TYPE = "type";
    public static final String PROPERTY_TABLE_TYPES = "table-types";
    public static final String PROPERTY_DATA_SOURCE = "data-source";
    public static final String PROPERTY_TABLE_DEFS = "table-defs";

    private final Map<String, Object> map;

    public DataContextPropertiesImpl() {
        this(new HashMap<String, Object>());
    }

    public DataContextPropertiesImpl(Properties properties) {
        this();
        final Set<String> propertyNames = properties.stringPropertyNames();
        for (String key : propertyNames) {
            put(key, properties.get(key));
        }
    }

    public DataContextPropertiesImpl(Map<String, Object> map) {
        this.map = map;
    }

    public Object get(String key) {
        return map.get(key);
    }

    public Object put(String key, Object value) {
        return map.put(key, value);
    }

    public String getString(String key) {
        final Object value = map.get(key);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    public Character getChar(String key) {
        final String str = getString(key);
        if (str == null || str.isEmpty()) {
            return null;
        }
        return str.charAt(0);
    }

    public Integer getInt(String key) {
        final Object obj = get(key);
        if (obj == null) {
            return null;
        }
        return NumberComparator.toNumber(obj).intValue();
    }

    private Boolean getBoolean(String key) {
        final Object obj = get(key);
        if (obj == null) {
            return null;
        }
        return BooleanComparator.toBoolean(obj);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        final Object obj = get(key);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        }
        if (obj instanceof String) {
            // TODO: Try parse as JSON
        }
        throw new IllegalStateException("Expected Map value for property '" + key + "'. Found " + obj.getClass()
                .getName());
    }

    @Override
    public String getDataContextType() {
        return getString(PROPERTY_DATA_CONTEXT_TYPE);
    }

    public void setDataContextType(String type) {
        put(PROPERTY_DATA_CONTEXT_TYPE, type);
    }

    @Override
    public Map<String, Object> toMap() {
        return map;
    }

    @Override
    public ResourceProperties getResourceProperties() {
        final Object resourceValue = get(PROPERTY_RESOURCE_PROPERTIES);
        if (resourceValue == null) {
            return null;
        }
        if (resourceValue instanceof String) {
            return new SimpleResourceProperties((String) resourceValue);
        }
        if (resourceValue instanceof URI) {
            return new SimpleResourceProperties((URI) resourceValue);
        }
        if (resourceValue instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> resourceMap = (Map<String, Object>) resourceValue;
            return new ResourcePropertiesImpl(resourceMap);
        }
        throw new IllegalStateException("Expected String, URI or Map value for property 'resource'. Found: "
                + resourceValue);
    }

    @Override
    public Integer getColumnNameLineNumber() {
        return getInt(PROPERTY_COLUMN_NAME_LINE_NUMBER);
    }

    @Override
    public Boolean isSkipEmptyLines() {
        return getBoolean(PROPERTY_SKIP_EMPTY_LINES);
    }

    @Override
    public Boolean isSkipEmptyColumns() {
        return getBoolean(PROPERTY_SKIP_EMPTY_COLUMNS);
    }

    @Override
    public String getEncoding() {
        return getString(PROPERTY_ENCODING);
    }

    @Override
    public Character getSeparatorChar() {
        return getChar(PROPERTY_SEPARATOR_CHAR);
    }

    @Override
    public Character getQuoteChar() {
        return getChar(PROPERTY_QUOTE_CHAR);
    }

    @Override
    public Character getEscapeChar() {
        return getChar(PROPERTY_ESCAPE_CHAR);
    }

    @Override
    public Boolean isFailOnInconsistentRowLength() {
        return getBoolean(PROPERTY_IS_FAIL_ON_INCONSISTENT_ROW_LENGTH);
    }

    @Override
    public Boolean isMultilineValuesEnabled() {
        return getBoolean(PROPERTY_IS_MULTILINE_VALUES_ENABLED);
    }

    @Override
    public TableType[] getTableTypes() {
        final Object obj = get(PROPERTY_TABLE_TYPES);
        if (obj == null) {
            return null;
        }
        if (obj instanceof TableType[]) {
            return (TableType[]) obj;
        }
        if (obj instanceof TableType) {
            return new TableType[] { (TableType) obj };
        }
        if (obj instanceof String) {
            String str = (String) obj;
            if (str.startsWith("[") && str.endsWith("]")) {
                str = str.substring(1, str.length() - 2);
            }
            final String[] tokens = str.split(",");
            final TableType[] tableTypes = new TableType[tokens.length];
            for (int i = 0; i < tableTypes.length; i++) {
                tableTypes[i] = TableType.getTableType(tokens[i]);
            }
        }
        throw new IllegalStateException("Expected TableType[] value for property '" + PROPERTY_TABLE_TYPES + "'. Found "
                + obj.getClass().getName());
    }

    @Override
    public String getCatalogName() {
        return getString(PROPERTY_CATALOG_NAME);
    }

    @Override
    public String getUrl() {
        return getString(PROPERTY_URL);
    }

    @Override
    public DataSource getDataSource() {
        return (DataSource) get(PROPERTY_DATA_SOURCE);
    }

    @Override
    public String getUsername() {
        return getString(PROPERTY_USERNAME);
    }

    @Override
    public String getPassword() {
        return getString(PROPERTY_PASSWORD);
    }

    @Override
    public String getDriverClassName() {
        return getString(PROPERTY_DRIVER_CLASS);
    }

    @Override
    public String getHostname() {
        return getString(PROPERTY_HOSTNAME);
    }

    @Override
    public Integer getPort() {
        return getInt(PROPERTY_PORT);
    }

    @Override
    public String getDatabaseName() {
        return getString(PROPERTY_DATABASE);
    }

    @Override
    public SimpleTableDef[] getTableDefs() {
        final Object obj = get(PROPERTY_TABLE_DEFS);
        if (obj instanceof SimpleTableDef[]) {
            return (SimpleTableDef[]) obj;
        }
        if (obj instanceof SimpleTableDef) {
            return new SimpleTableDef[] { (SimpleTableDef) obj };
        }
        if (obj instanceof String) {
            return SimpleTableDefParser.parseTableDefs((String) obj);
        }
        throw new IllegalStateException("Expected SimpleTableDef[] value for property '" + PROPERTY_TABLE_DEFS
                + "'. Found " + obj.getClass().getName());
    }

}

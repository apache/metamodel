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
import org.apache.metamodel.util.SimpleTableDef;

public class DataContextPropertiesImpl implements DataContextProperties {

    private static final long serialVersionUID = 1L;

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
        return (Integer) get(key);
    }

    private Boolean getBoolean(String key) {
        return (Boolean) get(key);
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getMap(String key) {
        return (Map<String, Object>) get(key);
    }

    @Override
    public String getDataContextType() {
        return getString("type");
    }

    @Override
    public Map<String, Object> toMap() {
        return map;
    }

    @Override
    public ResourceProperties getResourceProperties() {
        final Object resourceValue = get("resource");
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
        throw new IllegalStateException("Unsupported 'resource' definition: " + resourceValue);
    }

    @Override
    public Integer getColumnNameLineNumber() {
        return getInt("column-name-line-number");
    }

    @Override
    public Boolean isSkipEmptyLines() {
        return getBoolean("skip-empty-lines");
    }

    @Override
    public Boolean isSkipEmptyColumns() {
        return getBoolean("skip-empty-columns");
    }

    @Override
    public String getEncoding() {
        return getString("encoding");
    }

    @Override
    public Character getSeparatorChar() {
        return getChar("separator-char");
    }

    @Override
    public Character getQuoteChar() {
        return getChar("quote-char");
    }

    @Override
    public Character getEscapeChar() {
        return getChar("escape-char");
    }

    @Override
    public Boolean isFailOnInconsistentRowLength() {
        return getBoolean("fail-on-inconsistent-row-length");
    }

    @Override
    public Boolean isMultilineValuesEnabled() {
        return getBoolean("multiline-values");
    }

    @Override
    public TableType[] getTableTypes() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCatalogName() {
        return getString("catalog");
    }

    @Override
    public String getUrl() {
        return getString("url");
    }

    @Override
    public DataSource getDataSource() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getUsername() {
        return getString("username");
    }

    @Override
    public String getPassword() {
        return getString("password");
    }

    @Override
    public String getDriverClassName() {
        return getString("driver-class");
    }

    @Override
    public String getHostname() {
        return getString("hostname");
    }

    @Override
    public Integer getPort() {
        return getInt("port");
    }

    @Override
    public String getDatabaseName() {
        return getString("database");
    }

    @Override
    public SimpleTableDef[] getTableDefs() {
        // TODO Auto-generated method stub
        return null;
    }

}

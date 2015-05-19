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
package org.apache.metamodel.jdbc.integrationtests;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.util.FileHelper;

import junit.framework.TestCase;

/**
 * Convenience super class for integration {@link TestCase}s.
 */
public abstract class AbstractJdbIntegrationTest extends TestCase {

    private Properties _properties;
    private boolean _configured;
    private String _url;
    private String _username;
    private String _password;
    private String _driver;
    private Connection _connection;

    @Override
    protected final void setUp() throws Exception {
        super.setUp();

        // create property prefix of the form "jdbc.databasetype.property"
        final String propertyPrefix = "jdbc." + getPropertyPrefix();

        _properties = new Properties();
        final File file = new File(getPropertyFilePath());
        if (file.exists()) {
            _properties.load(new FileReader(file));
            _url = _properties.getProperty(propertyPrefix + ".url");
            _driver = _properties.getProperty(propertyPrefix + ".driver");
            _username = _properties.getProperty(propertyPrefix + ".username");
            _password = _properties.getProperty(propertyPrefix + ".password");
            _configured = _url != null && _driver != null;
        } else {
            _configured = false;
        }
    }

    protected Properties getProperties() {
        return _properties;
    }

    protected String getDriver() {
        return _driver;
    }

    protected String getUsername() {
        return _username;
    }

    protected String getPassword() {
        return _password;
    }

    protected String getUrl() {
        return _url;
    }

    @Override
    protected final void tearDown() throws Exception {
        FileHelper.safeClose(_connection);
        _connection = null;
    }

    public boolean isConfigured() {
        return _configured;
    }

    protected abstract String getPropertyPrefix();

    protected Connection getConnection() {
        final String className = getClass().getName();

        if (!_configured) {
            throw new IllegalStateException(className + " is not properly configured from file: "
                    + getPropertyFilePath());
        }

        if (_connection == null) {
            try {
                Class.forName(_driver);
                _connection = DriverManager.getConnection(_url, _username, _password);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to create JDBC connection for " + className, e);
            }
        }

        return _connection;
    }

    protected JdbcDataContext getDataContext() {
        return new JdbcDataContext(getConnection());
    }

    protected BasicDataSource getDataSource() {
        final BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(getUrl());
        dataSource.setDriverClassName(getDriver());
        dataSource.setUsername(getUsername());
        dataSource.setPassword(getPassword());

        return dataSource;
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }
}

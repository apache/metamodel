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
package org.apache.metamodel.mongodb.mongo3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

public abstract class MongoDbTestCase extends TestCase {

    private static final String DEFAULT_TEST_COLLECTION_NAME = "my_collection";
    private static final String DEFAULT_TEST_DATABASE_NAME = "metamodel_test";

    private String _hostname;
    private String _collectionName;
    private boolean _configured;

    private String _databaseName;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        File file = new File(getPropertyFilePath());
        if (file.exists()) {
            loadPropertyFile(file);
        } else {
            // Continuous integration case
            if (System.getenv("CONTINUOUS_INTEGRATION") != null) {
                File travisFile = new File("../travis-metamodel-integrationtest-configuration.properties");
                if (travisFile.exists()) {
                    loadPropertyFile(travisFile);
                } else {
                    _configured = false;
                }
            } else {
                _configured = false;
            }
        }
    }

    private void loadPropertyFile(File file) throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        properties.load(new FileReader(file));
        _hostname = properties.getProperty("mongodb.hostname");
        
        _databaseName = properties.getProperty("mongodb.databaseName");
        if (_databaseName == null || _databaseName.isEmpty()) {
            _databaseName = DEFAULT_TEST_DATABASE_NAME;
        }
        
        _collectionName = properties.getProperty("mongodb.collectionName");
        if (_collectionName == null || _collectionName.isEmpty()) {
            _collectionName = DEFAULT_TEST_COLLECTION_NAME;
        }

        _configured = (_hostname != null && !_hostname.isEmpty());

        if (_configured) {
            System.out.println("Loaded MongoDB configuration. Hostname=" + _hostname + ", Collection="
                    + _collectionName);
        }
        
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! MongoDB module ignored\r\n" + "Please configure mongodb connection locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }
    
    public String getDatabaseName() {
        return _databaseName;
    }

    public String getCollectionName() {
        return _collectionName;
    }

    public String getHostname() {
        return _hostname;
    }
    
    public boolean isConfigured() {
        return _configured;
    }
}

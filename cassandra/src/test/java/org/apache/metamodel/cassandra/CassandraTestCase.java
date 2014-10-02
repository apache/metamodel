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
package org.apache.metamodel.cassandra;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

public abstract class CassandraTestCase extends TestCase {

    private static final String DEFAULT_TEST_KEYSPACE_NAME = "my_keyspace";
    private static final Integer DEFAULT_TEST_PORT = 9042;

    private String _hostname;
    private Integer _port;
    private String _keySpaceName;
    private boolean _configured;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Properties properties = new Properties();
        File file = new File(getPropertyFilePath());
        if (file.exists()) {
            properties.load(new FileReader(file));
            _hostname = properties.getProperty("cassandra.hostname");

            _keySpaceName = properties.getProperty("cassandra.keyspace");
            if (_keySpaceName == null || _keySpaceName.isEmpty()) {
                _keySpaceName = DEFAULT_TEST_KEYSPACE_NAME;
            }
            
            _port = new Integer(properties.getProperty("cassandra.port"));
            if (_port == null) {
                _port = DEFAULT_TEST_PORT;
            }

            _configured = (_hostname != null && !_hostname.isEmpty());

            if (_configured) {
                System.out.println("Loaded Cassandra configuration. Hostname=" + _hostname + ", Keyspace="
                        + _keySpaceName);
            }
        } else {
            _configured = false;
        }
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! Cassandra module ignored\r\n" + "Please configure cassandra connection locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }

    public String getHostname() {
        return _hostname;
    }
    
    public String getKeyspaceName() {
        return _keySpaceName;
    }

    public Integer getPort() {
        return _port;
    }

    public boolean isConfigured() {
        return _configured;
    }
}

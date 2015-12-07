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
package org.apache.metamodel.neo4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

public abstract class Neo4jTestCase extends TestCase {

    private String _hostname;
    private int _port = Neo4jDataContext.DEFAULT_PORT;
    private String _username;
    private String _password;
    private String _serviceRoot = "/db/data";
    private boolean _configured;

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

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! Neo4j module ignored\n" + "Please configure Neo4j connection locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }

    private void loadPropertyFile(File file) throws IOException, FileNotFoundException {
        Properties properties = new Properties();
        properties.load(new FileReader(file));
        _hostname = properties.getProperty("neo4j.hostname");
        String portString = properties.getProperty("neo4j.port");
        if (portString != null) {
            _port = Integer.parseInt(portString);
        }
        _username = properties.getProperty("neo4j.username");
        _password = properties.getProperty("neo4j.password");
        String serviceRoot = properties.getProperty("neo4j.serviceroot");
        if (serviceRoot != null) {
            _serviceRoot = serviceRoot;
        }

        _configured = (_hostname != null && !_hostname.isEmpty());

        if (_configured) {
            System.out.println("Loaded Neo4j configuration. Hostname=" + _hostname + ", port=" + _port + ", username="
                    + _username + ", _serviceRoot=" + _serviceRoot);
        }
    }

    public boolean isConfigured() {
        return _configured;
    }

    public String getHostname() {
        return _hostname;
    }

    public int getPort() {
        return _port;
    }

    public String getUsername() {
        return _username;
    }

    public String getPassword() {
        return _password;
    }

    public String getServiceRoot() {
        return _serviceRoot;
    }

}

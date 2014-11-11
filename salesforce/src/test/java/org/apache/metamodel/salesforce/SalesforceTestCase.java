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
package org.apache.metamodel.salesforce;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import com.sforce.soap.partner.Connector;
import junit.framework.TestCase;

/**
 * Abstract test case which consults an external .properties file for SFDC
 * credentials to execute the tests.
 */
public abstract class SalesforceTestCase extends TestCase {

    private String _username;
    private String _password;
    private String _securityToken;
    private String _endpoint;
    private boolean _configured;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Properties properties = new Properties();
        File file = new File(getPropertyFilePath());
        if (file.exists()) {
            properties.load(new FileReader(file));
            _username = properties.getProperty("salesforce.username");
            _password = properties.getProperty("salesforce.password");
            _securityToken = properties.getProperty("salesforce.securityToken");
            _endpoint = properties.getProperty("salesforce.endpoint", Connector.END_POINT);

            _configured = (_username != null && !_username.isEmpty());
        } else {
            _configured = false;
        }
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! Salesforce module ignored\r\n" + "Please configure salesforce credentials locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }

    public boolean isConfigured() {
        return _configured;
    }

    public String getUsername() {
        return _username;
    }

    public String getPassword() {
        return _password;
    }

    public String getSecurityToken() {
        return _securityToken;
    }

    public String getEndpoint() {
        return _endpoint;
    }

}

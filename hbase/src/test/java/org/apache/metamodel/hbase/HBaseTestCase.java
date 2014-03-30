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
package org.apache.metamodel.hbase;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;

import junit.framework.TestCase;

public abstract class HBaseTestCase extends TestCase {

    private String zookeeperHostname;
    private int zookeeperPort;
    private boolean _configured;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Properties properties = new Properties();
        File file = new File(getPropertyFilePath());
        if (file.exists()) {
            properties.load(new FileReader(file));
            zookeeperHostname = properties.getProperty("hbase.zookeeper.hostname");
            String zookeeperPortPropertyValue = properties.getProperty("hbase.zookeeper.port");
            if (zookeeperPortPropertyValue != null && !zookeeperPortPropertyValue.isEmpty()) {
                zookeeperPort = Integer.parseInt(zookeeperPortPropertyValue);
            }
            
            _configured = (zookeeperHostname != null && !zookeeperHostname.isEmpty());
        } else {
            _configured = false;
        }
    }
    
    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! HBase module ignored\r\n" + "Please configure HBase properties locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }

    public boolean isConfigured() {
        return _configured;
    }
    
    public String getZookeeperHostname() {
        return zookeeperHostname;
    }

    public int getZookeeperPort() {
        return zookeeperPort;
    }
}

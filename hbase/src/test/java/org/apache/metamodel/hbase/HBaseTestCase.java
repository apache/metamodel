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

import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.metamodel.schema.ColumnType;
import org.junit.AfterClass;

/**
 * Properly configure before executing these tests.
 * See the {@link HBaseTestCase#setUp()} and {@link HBaseTestCase#getPropertyFilePath()} methods.
 */
public abstract class HBaseTestCase {

    // TableName
    protected static final String TABLE_NAME = "table_for_junit";

    // ColumnFamilies
    protected static final int NUMBER_OF_CFS = 3; // foo + bar + ID
    protected static final String CF_FOO = "foo";
    protected static final String CF_BAR = "bar";

    // Qualifiers
    protected static final String Q_HELLO = "hello";
    protected static final String Q_HI = "hi";
    protected static final String Q_HEY = "hey";
    protected static final String Q_BAH = "bah";

    // Number of rows
    protected static final int NUMBER_OF_ROWS = 2;

    // RowKeys
    protected static final String RK_1 = "junit1";
    protected static final String RK_2 = "junit2";

    // RowValues
    protected static final String V_WORLD = "world";
    protected static final String V_THERE = "there";
    protected static final String V_YO = "yo";
    protected static final byte[] V_123_BYTE_ARRAY = new byte[] { 1, 2, 3 };
    protected static final String V_YOU = "you";

    private String zookeeperHostname;
    private int zookeeperPort;
    private static HBaseDataContext _dataContext;

    protected void setUp() throws Exception {
        boolean configured = false;

        Properties properties = new Properties();
        File file = new File(getPropertyFilePath());
        if (file.exists()) {
            properties.load(new FileReader(file));
            zookeeperHostname = properties.getProperty("hbase.zookeeper.hostname");
            String zookeeperPortPropertyValue = properties.getProperty("hbase.zookeeper.port");
            if (zookeeperPortPropertyValue != null && !zookeeperPortPropertyValue.isEmpty()) {
                zookeeperPort = Integer.parseInt(zookeeperPortPropertyValue);
            }

            configured = (zookeeperHostname != null && !zookeeperHostname.isEmpty());
        }
        assumeTrue(configured);

        final HBaseConfiguration configuration = new HBaseConfiguration(zookeeperHostname, zookeeperPort,
                ColumnType.VARCHAR);
        setDataContext(new HBaseDataContext(configuration));
    }

    @AfterClass
    public static void oneTimeTeardown() throws IOException {
        if (_dataContext != null) {
            _dataContext.getConnection().close();
        }
    }

    /**
     * Gets the test configuration file. An example file can be found at the root folder of this project.
     * 
     * @return Location of the configuration file.
     */
    protected String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    protected String getInvalidConfigurationMessage() {
        return "!!! WARN !!! HBase module ignored\r\n" + "Please configure HBase properties locally ("
                + getPropertyFilePath() + "), to run integration tests";
    }

    public String getZookeeperHostname() {
        return zookeeperHostname;
    }

    public int getZookeeperPort() {
        return zookeeperPort;
    }

    public HBaseDataContext getDataContext() {
        return _dataContext;
    }

    public void setDataContext(HBaseDataContext dataContext) {
        HBaseTestCase._dataContext = dataContext;
    }
}

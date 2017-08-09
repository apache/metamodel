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

import java.io.Serializable;
import java.util.List;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.BaseObject;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * Represents the configuration of MetaModel's HBase adaptor.
 */
public class HBaseConfiguration extends BaseObject implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ColumnType DEFAULT_ROW_KEY_TYPE = ColumnType.BINARY;
    public static final String DEFAULT_SCHEMA_NAME = "HBase";
    public static final String DEFAULT_ZOOKEEPER_HOSTNAME = "127.0.0.1";
    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    public static final int DEFAULT_HBASE_CLIENT_RETRIES = 1;
    public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = 5000;
    public static final int DEFAULT_ZOOKEEPER_RECOVERY_RETRIES = 1;

    private final String _schemaName;
    private final int _zookeeperPort;
    private final String _zookeeperHostname;
    private final SimpleTableDef[] _tableDefinitions;
    private final ColumnType _defaultRowKeyType;
    private final int _hbaseClientRetries;
    private final int _zookeeperSessionTimeout;
    private final int _zookeeperRecoveryRetries;

    /**
     * Creates a {@link HBaseConfiguration} using default values.
     */
    public HBaseConfiguration() {
        this(DEFAULT_ZOOKEEPER_HOSTNAME, DEFAULT_ZOOKEEPER_PORT);
    }

    public HBaseConfiguration(String zookeeperHostname, int zookeeperPort) {
        this(DEFAULT_SCHEMA_NAME, zookeeperHostname, zookeeperPort, null, DEFAULT_ROW_KEY_TYPE);
    }

    public HBaseConfiguration(String zookeeperHostname, int zookeeperPort, ColumnType defaultRowKeyType) {
        this(DEFAULT_SCHEMA_NAME, zookeeperHostname, zookeeperPort, null, defaultRowKeyType);
    }

    /**
     * Creates a {@link HBaseConfiguration} using detailed configuration
     * properties.
     * 
     * @param schemaName
     * @param zookeeperHostname
     * @param zookeeperPort
     * @param tableDefinitions
     * @param defaultRowKeyType
     */
    public HBaseConfiguration(String schemaName, String zookeeperHostname, int zookeeperPort,
            SimpleTableDef[] tableDefinitions, ColumnType defaultRowKeyType) {
        this(schemaName, zookeeperHostname, zookeeperPort, tableDefinitions, defaultRowKeyType,
                DEFAULT_HBASE_CLIENT_RETRIES, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT, DEFAULT_ZOOKEEPER_RECOVERY_RETRIES);
    }

    /**
     * Creates a {@link HBaseConfiguration} using detailed configuration
     * properties.
     * 
     * @param schemaName
     * @param zookeeperHostname
     * @param zookeeperPort
     * @param tableDefinitions
     * @param defaultRowKeyType
     * @param hbaseClientRetries
     * @param zookeeperSessionTimeout
     * @param zookeeperRecoveryRetries
     */
    public HBaseConfiguration(String schemaName, String zookeeperHostname, int zookeeperPort,
            SimpleTableDef[] tableDefinitions, ColumnType defaultRowKeyType, int hbaseClientRetries,
            int zookeeperSessionTimeout, int zookeeperRecoveryRetries) {
        _schemaName = schemaName;
        _zookeeperHostname = zookeeperHostname;
        _zookeeperPort = zookeeperPort;
        _tableDefinitions = tableDefinitions;
        _defaultRowKeyType = defaultRowKeyType;
        _hbaseClientRetries = hbaseClientRetries;
        _zookeeperSessionTimeout = zookeeperSessionTimeout;
        _zookeeperRecoveryRetries = zookeeperRecoveryRetries;
    }

    public String getSchemaName() {
        return _schemaName;
    }

    public String getZookeeperHostname() {
        return _zookeeperHostname;
    }

    public int getZookeeperPort() {
        return _zookeeperPort;
    }

    public SimpleTableDef[] getTableDefinitions() {
        return _tableDefinitions;
    }

    public ColumnType getDefaultRowKeyType() {
        return _defaultRowKeyType;
    }

    @Override
    protected void decorateIdentity(List<Object> list) {
        list.add(_schemaName);
        list.add(_zookeeperHostname);
        list.add(_zookeeperPort);
        list.add(_tableDefinitions);
        list.add(_defaultRowKeyType);
    }

    public int getHBaseClientRetries() {
        return _hbaseClientRetries;
    }

    public int getZookeeperSessionTimeout() {
        return _zookeeperSessionTimeout;
    }

    public int getZookeeperRecoveryRetries() {
        return _zookeeperRecoveryRetries;
    }
}

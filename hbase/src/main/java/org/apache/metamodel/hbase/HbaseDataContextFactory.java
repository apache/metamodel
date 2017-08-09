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

import java.util.Map;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.AbstractDataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.ColumnTypeImpl;
import org.apache.metamodel.util.SimpleTableDef;

public class HbaseDataContextFactory extends AbstractDataContextFactory {

    @Override
    protected String getType() {
        return "hbase";
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        final Map<String, Object> map = properties.toMap();

        final String schemaName = getString(map.get("schema"),
                getString(properties.getDatabaseName(), HBaseConfiguration.DEFAULT_SCHEMA_NAME));
        final String zookeeperHostname =
                getString(map.get("zookeeper-hostname"), HBaseConfiguration.DEFAULT_ZOOKEEPER_HOSTNAME);
        final int zookeeperPort = getInt(map.get("zookeeper-port"), HBaseConfiguration.DEFAULT_ZOOKEEPER_PORT);
        final SimpleTableDef[] tableDefinitions = properties.getTableDefs();

        final ColumnType defaultRowKeyType;
        final Object configuredDefaultRowKeyType = map.get("default-row-key-type");
        if (configuredDefaultRowKeyType == null) {
            defaultRowKeyType = HBaseConfiguration.DEFAULT_ROW_KEY_TYPE;
        } else if (configuredDefaultRowKeyType instanceof ColumnType) {
            defaultRowKeyType = (ColumnType) configuredDefaultRowKeyType;
        } else if (configuredDefaultRowKeyType instanceof String && !((String) configuredDefaultRowKeyType).isEmpty()) {
            defaultRowKeyType = ColumnTypeImpl.valueOf((String) configuredDefaultRowKeyType);
        } else {
            defaultRowKeyType = HBaseConfiguration.DEFAULT_ROW_KEY_TYPE;
        }

        final int hbaseClientRetries =
                getInt(map.get("hbase-client-retries"), HBaseConfiguration.DEFAULT_HBASE_CLIENT_RETRIES);
        final int zookeeperSessionTimeout =
                getInt(map.get("zookeeper-session-timeout"), HBaseConfiguration.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT);
        final int zookeeperRecoveryRetries =
                getInt(map.get("zookeeper-recovery-retries"), HBaseConfiguration.DEFAULT_ZOOKEEPER_RECOVERY_RETRIES);

        final HBaseConfiguration configuration =
                new HBaseConfiguration(schemaName, zookeeperHostname, zookeeperPort, tableDefinitions,
                        defaultRowKeyType, hbaseClientRetries, zookeeperSessionTimeout, zookeeperRecoveryRetries);
        return new HBaseDataContext(configuration);
    }

}

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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * MetaModel adaptor for Apache HBase.
 */
public class HBaseDataContext extends QueryPostprocessDataContext {

    public static final String FIELD_ID = "_id";

    private final HBaseConfiguration _configuration;
    private final HBaseAdmin _admin;
    private final HTablePool _tablePool;

    /**
     * Creates a {@link HBaseDataContext}.
     * 
     * @param configuration
     */
    public HBaseDataContext(HBaseConfiguration configuration) {
        Configuration config = createConfig(configuration);
        _configuration = configuration;
        _admin = createHbaseAdmin(config);
        _tablePool = new HTablePool(config, 100);
    }

    /**
     * Creates a {@link HBaseDataContext}.
     * 
     * @param configuration
     * @param admin
     * @param hTablePool
     */
    public HBaseDataContext(HBaseConfiguration configuration, HBaseAdmin admin, HTablePool hTablePool) {
        _configuration = configuration;
        _tablePool = hTablePool;
        _admin = admin;
    }

    private HBaseAdmin createHbaseAdmin(Configuration config) {
        try {
            return new HBaseAdmin(config);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new MetaModelException(e);
        }
    }

    private Configuration createConfig(HBaseConfiguration configuration) {
        Configuration config = org.apache.hadoop.hbase.HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", configuration.getZookeeperHostname());
        config.set("hbase.zookeeper.property.clientPort", Integer.toString(configuration.getZookeeperPort()));
        return config;
    }

    public HTablePool getTablePool() {
        return _tablePool;
    }

    /**
     * Gets the HBaseAdmin used by this {@link DataContext}
     * 
     * @return
     */
    public HBaseAdmin getHBaseAdmin() {
        return _admin;
    }

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema schema = new MutableSchema(_configuration.getSchemaName());

        try {
            SimpleTableDef[] tableDefinitions = _configuration.getTableDefinitions();
            if (tableDefinitions == null) {
                final HTableDescriptor[] tables = _admin.listTables();
                tableDefinitions = new SimpleTableDef[tables.length];
                for (int i = 0; i < tables.length; i++) {
                    SimpleTableDef emptyTableDef = new SimpleTableDef(tables[i].getNameAsString(), new String[0]);
                    tableDefinitions[i] = emptyTableDef;
                }
            }

            for (SimpleTableDef tableDef : tableDefinitions) {
                schema.addTable(new HBaseTable(tableDef, schema, _admin, _configuration.getDefaultRowKeyType()));
            }

            return schema;
        } catch (Exception e) {
            throw new MetaModelException(e);
        }
    }

    /**
     * Gets the {@link HBaseConfiguration} that is used in this datacontext.
     * 
     * @return
     */
    public HBaseConfiguration getConfiguration() {
        return _configuration;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return _configuration.getSchemaName();
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (whereItems != null && !whereItems.isEmpty()) {
            return null;
        }

        long result = 0;
        final HTableInterface hTable = _tablePool.getTable(table.getName());
        try {
            ResultScanner scanner = hTable.getScanner(new Scan());
            try {
                while (scanner.next() != null) {
                    result++;
                }                
            } finally {
                scanner.close();
            }
            return result;
        } catch (IOException e) {
            throw new MetaModelException(e);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        final Scan scan = new Scan();
        for (Column column : columns) {
            if (!column.isPrimaryKey()) {
                final int colonIndex = column.getName().indexOf(':');
                if (colonIndex != -1) {
                    String family = column.getName().substring(0, colonIndex);
                    scan.addFamily(family.getBytes());
                } else {
                    scan.addFamily(column.getName().getBytes());
                }
            }
        }

        scan.setMaxResultSize(maxRows);

        final HTableInterface hTable = _tablePool.getTable(table.getName());
        try {
            final ResultScanner scanner = hTable.getScanner(scan);
            return new HBaseDataSet(columns, scanner, hTable);
        } catch (Exception e) {
            FileHelper.safeClose(hTable);
            throw new MetaModelException(e);
        }
    }

}

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
package org.apache.metamodel.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.insert.InsertInto;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.junit.Test;

public class JdbcUpdateCallbackTest {

    /**
     * This test will only work with certain transaction isolation levels. Both levels
     * {@link Connection#TRANSACTION_REPEATABLE_READ} and {@link Connection#TRANSACTION_SERIALIZABLE} should work fine,
     * but lower levels will cause issues because of the concurrent reading of the "n" variable which is also being
     * concurrently updated.
     */
    private final int ISOLATION_LEVEL = java.sql.Connection.TRANSACTION_REPEATABLE_READ;

    @Test
    public void testTransactionalUpdateScripts() throws Exception {
        DerbyTest.initDerbySettings(); 
        
        final BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
        dataSource.setUrl("jdbc:derby:target/temp_derby;create=true");
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(10);
        dataSource.setMaxWait(10000);
        dataSource.setMinEvictableIdleTimeMillis(1800000);
        dataSource.setMinIdle(0);
        dataSource.setMaxIdle(10);
        dataSource.setNumTestsPerEvictionRun(3);
        dataSource.setTimeBetweenEvictionRunsMillis(-1);
        dataSource.setDefaultTransactionIsolation(ISOLATION_LEVEL);

        final String tableName = "counter_table";
        final String columnName = "n";
        final JdbcDataContext dataContext = new JdbcDataContext(dataSource);
        dataContext.executeUpdate(new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                if (dataContext.getTableByQualifiedLabel(tableName) != null) {
                    callback.dropTable(tableName).execute();
                }
                callback.createTable(dataContext.getDefaultSchema(), tableName).withColumn(columnName)
                        .ofType(ColumnType.INTEGER).execute();
            }
        });

        final Table table = dataContext.getTableByQualifiedLabel(tableName);
        final Column col = table.getColumnByName(columnName);
        assertNotNull(col);

        // insert one record - this one record will be updated transactionally below
        dataContext.executeUpdate(new InsertInto(table).value(columnName, 0));

        final UpdateScript updateScript = new UpdateScript() {
            @Override
            public void run(UpdateCallback callback) {
                final int n = getCounterValue(callback.getDataContext(), table, col);
                callback.update(table).value(col, n + 1).execute();
            }
        };

        final int threadCount = 2;
        final int iterationsPerThread = 5;

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < iterationsPerThread; j++) {
                        int retries = 10;
                        while (retries > 0) {
                            try {
                                dataContext.executeUpdate(updateScript);
                                retries = 0;
                            } catch (RolledBackUpdateException e) {
                                retries--;
                                if (retries == 0) {
                                    throw e;
                                }
                            }
                        }
                    }
                }
            };
        }
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(threadCount * iterationsPerThread, getCounterValue(dataContext, table, col));
    }

    private int getCounterValue(DataContext dataContext, Table table, Column column) {
        final DataSet ds = dataContext.query().from(table).select(column).execute();
        try {
            ds.next();
            final Number n = (Number) ds.getRow().getValue(0);
            return n.intValue();
        } finally {
            ds.close();
        }
    }
}

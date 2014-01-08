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
package org.apache.metamodel.spring;

import java.sql.Connection;
import java.sql.DriverManager;

import javax.sql.DataSource;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.TableType;

/**
 * {@link DataContextFactoryBeanDelegate} for {@link JdbcDataContext}.
 */
public class JdbcDataContextFactoryBeanDelegate extends AbstractDataContextFactoryBeanDelegate {

    @Override
    public DataContext createDataContext(DataContextFactoryBean bean) {
        TableType[] tableTypes = bean.getTableTypes();
        if (tableTypes == null) {
            tableTypes = TableType.DEFAULT_TABLE_TYPES;
        }

        final DataSource dataSource = bean.getDataSource();

        if (dataSource == null) {
            final String driverClassName = getString(bean.getDriverClassName(), null);
            if (driverClassName != null) {
                try {
                    Class.forName(driverClassName);
                } catch (ClassNotFoundException e) {
                    logger.error("Failed to initialize JDBC driver class '" + driverClassName + "'!", e);
                }
            }

            final String url = bean.getUrl();
            final Connection connection;
            try {
                if (bean.getUsername() == null && bean.getPassword() == null) {
                    connection = DriverManager.getConnection(url);
                } else {
                    connection = DriverManager.getConnection(url, bean.getUsername(), bean.getPassword());
                }
            } catch (Exception e) {
                logger.error("Failed to get JDBC connection using URL: " + url, e);
                throw new IllegalStateException("Failed to get JDBC connection", e);
            }

            return new JdbcDataContext(connection, tableTypes, bean.getCatalogName());
        }

        return new JdbcDataContext(dataSource, tableTypes, bean.getCatalogName());
    }

}

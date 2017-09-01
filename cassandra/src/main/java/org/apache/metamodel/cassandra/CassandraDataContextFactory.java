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

import java.util.Map;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.AbstractDataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.google.common.base.Strings;

public class CassandraDataContextFactory extends AbstractDataContextFactory {

    @Override
    protected String getType() {
        return "cassandra";
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {

        final Map<String, Object> map = properties.toMap();
        final Builder clusterBuilder = Cluster.builder();

        final String hostname = properties.getHostname();
        if (!Strings.isNullOrEmpty(hostname)) {
            clusterBuilder.addContactPoints(hostname.split(","));
        }

        if (properties.getPort() != null) {
            clusterBuilder.withPort(properties.getPort());
        }

        if (map.containsKey("cluster-name")) {
            clusterBuilder.withClusterName((String) map.get("cluster-name"));
        }

        if (properties.getUsername() != null && properties.getPassword() != null) {
            clusterBuilder.withCredentials(properties.getUsername(), properties.getPassword());
        }

        final Cluster cluster = clusterBuilder.build();

        final String keySpace = getString(map.get("keyspace"), properties.getDatabaseName());

        return new CassandraDataContext(cluster, keySpace, properties.getTableDefs());
    }

}

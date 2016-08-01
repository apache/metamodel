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
package org.apache.metamodel.elasticsearch.nativeclient;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

/**
 * Factory for ElasticSearch data context of native type.
 * 
 * The factory will activate when DataContext type is specified as
 * "elasticsearch", "es-node", "elasticsearch-node", "es-transport",
 * "elasticsearch-transport".
 * 
 * This factory is configured with the following properties:
 * 
 * <ul>
 * <li>clientType (needed if datacontext type is just "elasticsearch" - must be
 * either "transport" or "node")</li>
 * <li>hostname (if clientType is "transport")</li>
 * <li>port (if clientType is "transport")</li>
 * <li>database (index name)</li>
 * <li>cluster</li>
 * <li>username (optional, only available if clientType is "transport")</li>
 * <li>password (optional, only available if clientType is "transport")</li>
 * <li>ssl (optional, only available if clientType is "transport")</li>
 * <li>keystorePath (optional, only available if clientType is "transport")</li>
 * <li>keystorePassword (optional, only available if clientType is "transport")
 * </li>
 * </ul>
 */
public class ElasticSearchDataContextFactory implements DataContextFactory {

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        switch (properties.getDataContextType()) {
        case "elasticsearch":
        case "es-node":
        case "elasticsearch-node":
        case "es-transport":
        case "elasticsearch-transport":
            return acceptsInternal(properties);
        }
        return false;
    }

    private boolean acceptsInternal(DataContextProperties properties) {
        final String clientType = getClientType(properties);
        if (clientType == null) {
            return false;
        }
        if (!"node".equals(clientType)) {
            if (properties.getHostname() == null || properties.getPort() == null) {
                return false;
            }
        }
        if (getIndex(properties) == null) {
            return false;
        }
        if (getCluster(properties) == null) {
            return false;
        }
        return true;
    }

    private String getClientType(DataContextProperties properties) {
        switch (properties.getDataContextType()) {
        case "elasticsearch-node":
        case "es-node":
            return "node";
        case "elasticsearch-transport":
        case "es-transport":
            return "transport";
        }
        final String clientType = (String) properties.toMap().get("clientType");
        return clientType;
    }

    private String getIndex(DataContextProperties properties) {
        final String databaseName = properties.getDatabaseName();
        if (databaseName == null) {
            return (String) properties.toMap().get("index");
        }
        return databaseName;
    }

    private String getCluster(DataContextProperties properties) {
        return (String) properties.toMap().get("cluster");
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        final String clientType = getClientType(properties);
        final Client client;
        if ("node".equals(clientType)) {
            client = createNodeClient(properties);
        } else {
            client = createTransportClient(properties);
        }
        final String indexName = getIndex(properties);
        final SimpleTableDef[] tableDefinitions = properties.getTableDefs();
        return new ElasticSearchDataContext(client, indexName, tableDefinitions);
    }

    private Client createTransportClient(DataContextProperties properties) {
        final Builder settingsBuilder = ImmutableSettings.builder();
        settingsBuilder.put("name", "MetaModel");
        settingsBuilder.put("cluster.name", getCluster(properties));
        if (properties.getUsername() != null && properties.getPassword() != null) {
            settingsBuilder.put("shield.user", properties.getUsername() + ":" + properties.getPassword());
            if ("true".equals(properties.toMap().get("ssl"))) {
                if (properties.toMap().get("keystorePath") != null) {
                    settingsBuilder.put("shield.ssl.keystore.path", properties.toMap().get("keystorePath"));
                    settingsBuilder.put("shield.ssl.keystore.password", properties.toMap().get("keystorePassword"));
                }
                settingsBuilder.put("shield.transport.ssl", "true");
            }
        }
        final Settings settings = settingsBuilder.build();

        final TransportClient client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(properties.getHostname(), properties.getPort()));
        return client;
    }

    private Client createNodeClient(DataContextProperties properties) {
        final Builder settingsBuilder = ImmutableSettings.builder();
        settingsBuilder.put("name", "MetaModel");
        settingsBuilder.put("shield.enabled", false);
        final Settings settings = settingsBuilder.build();
        final Node node = NodeBuilder.nodeBuilder().clusterName(getCluster(properties)).client(true).settings(settings)
                .node();
        return node.client();
    }
}

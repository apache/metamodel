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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDataContextFactory.class);

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
        final Client client;
            client = createTransportClient(properties);
        final String indexName = getIndex(properties);
        final SimpleTableDef[] tableDefinitions = properties.getTableDefs();
        return new ElasticSearchDataContext(client, indexName, tableDefinitions);
    }

    private Client createTransportClient(DataContextProperties properties) {
        final Settings settings = Settings.builder().put().put("name", "MetaModel").put("cluster.name", getCluster(properties)).build();
        final TransportClient client = new PreBuiltTransportClient(settings);
        try {
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(properties.getHostname()), properties.getPort()));
        } catch (UnknownHostException e) {
            logger.warn("no IP address for the host with name \"{}\" could be found.", properties.getHostname());
        }
        return client;
    }

}

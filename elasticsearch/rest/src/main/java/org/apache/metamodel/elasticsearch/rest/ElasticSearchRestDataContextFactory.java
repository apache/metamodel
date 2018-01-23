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
package org.apache.metamodel.elasticsearch.rest;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * Factory for ElasticSearch data context of REST type.
 * 
 * The factory will activate when DataContext type is specified as
 * "elasticsearch", "es-rest" or "elasticsearch-rest".
 * 
 * This factory is configured with the following properties:
 * 
 * <ul>
 * <li>url (http or https based base URL of elasticsearch)</li>
 * <li>database (index name)</li>
 * <li>username (optional)</li>
 * <li>password (optional)</li>
 * </ul>
 */
public class ElasticSearchRestDataContextFactory implements DataContextFactory {

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        switch (properties.getDataContextType()) {
        case "elasticsearch":
            // ensure that the url is http or https based to infer that this is
            // a REST based connection
            final String url = properties.getUrl();
            return url != null && url.startsWith("http") && acceptsInternal(properties);
        case "es-rest":
        case "elasticsearch-rest":
            return acceptsInternal(properties);
        }
        return false;
    }

    private boolean acceptsInternal(DataContextProperties properties) {
        if (properties.getUrl() == null) {
            return false;
        }
        if (getIndex(properties) == null) {
            return false;
        }
        return true;
    }

    private ElasticSearchRestClient createClient(final DataContextProperties properties) throws MalformedURLException {
        final URL url = new URL(properties.getUrl());
        final RestClientBuilder builder = RestClient.builder(new HttpHost(url.getHost(), url.getPort()));
        
        if (properties.getUsername() != null) {
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
			credentialsProvider.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials(properties.getUsername(), properties.getPassword()));

            builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(
                    credentialsProvider));
        }

        return new ElasticSearchRestClient(builder.build());
    }

    private String getIndex(DataContextProperties properties) {
        final String databaseName = properties.getDatabaseName();
        if (databaseName == null) {
            properties.toMap().get("index");
        }
        return databaseName;
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        try {
            ElasticSearchRestClient client = createClient(properties);
            final String indexName = getIndex(properties);
            final SimpleTableDef[] tableDefinitions = properties.getTableDefs();
            return new ElasticSearchRestDataContext(client, indexName, tableDefinitions);
        } catch (MalformedURLException e) {
            throw new UnsupportedDataContextPropertiesException(e);
        }
    }

}

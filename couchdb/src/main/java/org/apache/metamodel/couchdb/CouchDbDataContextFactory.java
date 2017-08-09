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
package org.apache.metamodel.couchdb;

import java.net.MalformedURLException;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.AbstractDataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.util.SimpleTableDef;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

public class CouchDbDataContextFactory extends AbstractDataContextFactory {

    @Override
    protected String getType() {
        return "couchdb";
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        final StdHttpClient.Builder httpClientBuilder = new StdHttpClient.Builder();

        final String url = properties.getUrl();
        if (url != null && !url.isEmpty()) {
            try {
                httpClientBuilder.url(url);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException(url, e);
            }
        } else {
            httpClientBuilder.host(properties.getHostname());
            httpClientBuilder.port(getInt(properties.getPort(), CouchDbDataContext.DEFAULT_PORT));
            httpClientBuilder.enableSSL(getBoolean(properties.toMap().get("ssl"), false));
        }

        httpClientBuilder.username(properties.getUsername());
        httpClientBuilder.password(properties.getPassword());

        final HttpClient httpClient = httpClientBuilder.build();
        final SimpleTableDef[] tableDefs = properties.getTableDefs();
        if (tableDefs != null && tableDefs.length > 0) {
            return new CouchDbDataContext(httpClient, tableDefs);
        }

        final String databaseNames = properties.getDatabaseName();
        if (databaseNames != null && !databaseNames.isEmpty()) {
            return new CouchDbDataContext(new StdCouchDbInstance(httpClient), databaseNames.split(","));
        }

        return new CouchDbDataContext(httpClient);
    }

}

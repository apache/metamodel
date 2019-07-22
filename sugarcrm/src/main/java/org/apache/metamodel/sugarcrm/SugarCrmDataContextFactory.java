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
package org.apache.metamodel.sugarcrm;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.AbstractDataContextFactory;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;

/**
 * {@link DataContextFactory} for SugarCRM.
 * 
 * Properties used for configuration are: url, username, password and database name (used for SugarCRM application
 * name).
 */
public class SugarCrmDataContextFactory extends AbstractDataContextFactory {

    @Override
    protected String getType() {
        return "sugarcrm";
    }

    @Override
    public DataContext create(final DataContextProperties properties,
            final ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {
        final String sugarCrmBaseUrl = properties.getUrl();
        final String username = properties.getUsername();
        final String password = properties.getPassword();
        final String applicationName = properties.getDatabaseName();
        return new SugarCrmDataContext(sugarCrmBaseUrl, username, password, applicationName);
    }
}

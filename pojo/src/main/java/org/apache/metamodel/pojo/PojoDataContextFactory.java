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
package org.apache.metamodel.pojo;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.util.SimpleTableDef;

public class PojoDataContextFactory implements DataContextFactory {

    public static final String PROPERTY_TYPE = "pojo";

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        return PROPERTY_TYPE.equals(properties.getDataContextType());
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {

        assert accepts(properties, resourceFactoryRegistry);

        final String schemaName;
        if (properties.getDatabaseName() != null) {
            schemaName = properties.getDatabaseName();
        } else {
            schemaName = "Schema";
        }

        final List<TableDataProvider<?>> tableDataProviders;

        final SimpleTableDef[] tableDefs = properties.getTableDefs();
        if (tableDefs == null) {
            tableDataProviders = new ArrayList<>();
        } else {
            tableDataProviders = new ArrayList<>(tableDefs.length);
            for (int i = 0; i < tableDefs.length; i++) {
                final TableDataProvider<?> tableDataProvider = new ArrayTableDataProvider(tableDefs[i],
                        new ArrayList<Object[]>());
                tableDataProviders.add(tableDataProvider);
            }
        }

        return new PojoDataContext(schemaName, tableDataProviders);
    }

}

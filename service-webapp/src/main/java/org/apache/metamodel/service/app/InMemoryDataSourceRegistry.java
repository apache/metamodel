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
package org.apache.metamodel.service.app;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.service.app.exceptions.DataSourceAlreadyExistException;
import org.apache.metamodel.service.app.exceptions.NoSuchDataSourceException;

public class InMemoryDataSourceRegistry implements DataSourceRegistry {

    private final Map<String, Supplier<DataContext>> dataSources;

    public InMemoryDataSourceRegistry() {
        dataSources = new LinkedHashMap<>();
    }

    @Override
    public String registerDataSource(final String name, final DataSourceDefinition dataSourceDef)
            throws DataSourceAlreadyExistException {
        if (dataSources.containsKey(name)) {
            throw new DataSourceAlreadyExistException(name);
        }

        dataSources.put(name, new DataContextSupplier(name, dataSourceDef));
        return name;
    }

    @Override
    public List<String> getDataSourceNames() {
        return dataSources.keySet().stream().collect(Collectors.toList());
    }

    @Override
    public DataContext openDataContext(String name) {
        final Supplier<DataContext> supplier = dataSources.get(name);
        if (supplier == null) {
            throw new NoSuchDataSourceException(name);
        }
        return supplier.get();
    }

}

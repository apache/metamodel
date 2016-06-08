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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.PojoDataContext;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;
import org.apache.metamodel.util.SimpleTableDefParser;

public class DataContextSupplier implements Supplier<DataContext> {

    private final String dataSourceName;
    private final DataSourceDefinition dataSourceDefinition;
    private final DataContext eagerLoadedDataContext;

    public DataContextSupplier(String dataSourceName, DataSourceDefinition dataSourceDefinition) {
        this.dataSourceName = dataSourceName;
        this.dataSourceDefinition = dataSourceDefinition;
        this.eagerLoadedDataContext = createDataContext(true);
    }

    @Override
    public DataContext get() {
        if (eagerLoadedDataContext != null) {
            return eagerLoadedDataContext;
        }
        return createDataContext(false);
    }

    private DataContext createDataContext(boolean eager) {
        final String type = dataSourceDefinition.getType();
        switch (type.toLowerCase()) {
        case "pojo":
            final List<TableDataProvider<?>> tableDataProviders;

            final Object tableDefinitions = dataSourceDefinition.getTableDefinitions();
            if (tableDefinitions == null) {
                tableDataProviders = new ArrayList<>(0);
            } else if (tableDefinitions instanceof String) {
                final SimpleTableDef[] tableDefs = SimpleTableDefParser.parseTableDefs((String) tableDefinitions);
                tableDataProviders = Arrays.stream(tableDefs).map((tableDef) -> {
                    return new ArrayTableDataProvider(tableDef, new ArrayList<Object[]>());
                }).collect(Collectors.toList());
            } else {
                throw new UnsupportedOperationException("Unsupported table definition type: " + tableDefinitions);
            }

            final String schemaName = dataSourceDefinition.getSchemaName() == null ? dataSourceName
                    : dataSourceDefinition.getSchemaName();

            return new PojoDataContext(schemaName, tableDataProviders);
        }

        if (eager) {
            return null;
        } else {
            throw new UnsupportedOperationException("Unsupported data source type: " + type);
        }
    }

}

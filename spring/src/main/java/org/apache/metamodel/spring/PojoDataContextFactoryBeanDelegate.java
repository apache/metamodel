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

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.pojo.ArrayTableDataProvider;
import org.apache.metamodel.pojo.PojoDataContext;
import org.apache.metamodel.pojo.TableDataProvider;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * {@link DataContextFactoryBeanDelegate} for {@link PojoDataContext}.
 */
public class PojoDataContextFactoryBeanDelegate extends AbstractDataContextFactoryBeanDelegate {

    @Override
    public DataContext createDataContext(DataContextFactoryParameters params) {
        SimpleTableDef[] tableDefs = params.getTableDefs();
        if (tableDefs == null || tableDefs.length == 0) {
            throw new IllegalArgumentException("Cannot create PojoDataContext with no tableDefs specified");
        }
        
        TableDataProvider<?>[] tableDataProviders = new ArrayTableDataProvider[tableDefs.length];
        for (int i = 0; i < tableDataProviders.length; i++) {
            ArrayTableDataProvider tableDataProvider = new ArrayTableDataProvider(tableDefs[i], new ArrayList<Object[]>());
            tableDataProviders[i] = tableDataProvider;
        }
        
        String databaseName = params.getDatabaseName();
        if (databaseName != null && !databaseName.isEmpty()) {
            return new PojoDataContext(databaseName, tableDataProviders);
        }
        return new PojoDataContext(Arrays.asList(tableDataProviders));
    }

}

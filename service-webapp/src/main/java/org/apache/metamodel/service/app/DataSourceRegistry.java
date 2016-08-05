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

import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.service.app.exceptions.DataSourceAlreadyExistException;
import org.apache.metamodel.service.app.exceptions.DataSourceNotUpdateableException;
import org.apache.metamodel.service.app.exceptions.NoSuchDataSourceException;

/**
 * Represents a user's/tenant's registry of {@link DataContext}s.
 */
public interface DataSourceRegistry {

    public List<String> getDataSourceNames();

    public String registerDataSource(String dataContextName, DataContextProperties dataContextProperties) throws DataSourceAlreadyExistException;

    public DataContext openDataContext(String dataSourceName) throws NoSuchDataSourceException;

    public default UpdateableDataContext openDataContextForUpdate(String dataSourceName) {
        final DataContext dataContext = openDataContext(dataSourceName);
        if (dataContext instanceof UpdateableDataContext) {
            return (UpdateableDataContext) dataContext;
        }
        throw new DataSourceNotUpdateableException(dataSourceName);
    };
}

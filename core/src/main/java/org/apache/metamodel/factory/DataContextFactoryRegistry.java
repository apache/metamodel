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
package org.apache.metamodel.factory;

import java.util.Collection;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;

/**
 * Represents a registry of {@link DataContextFactory} objects. This registry
 * can be used to create {@link DataContext}s of varying types using the
 * underlying factories.
 */
public interface DataContextFactoryRegistry {

    public void addFactory(DataContextFactory factory);

    public void clearFactories();

    public Collection<DataContextFactory> getFactories();

    public DataContext createDataContext(DataContextProperties properties)
            throws UnsupportedDataContextPropertiesException, ConnectionException;
}

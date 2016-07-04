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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.metamodel.DataContext;

public class DataContextFactoryRegistryImpl implements DataContextFactoryRegistry {

    private static final DataContextFactoryRegistry DEFAULT_INSTANCE;

    static {
        final DataContextFactoryRegistryImpl registry = new DataContextFactoryRegistryImpl();
        registry.discoverFromClasspath();
        DEFAULT_INSTANCE = registry;
    }

    public static DataContextFactoryRegistry getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    private final List<DataContextFactory> factories;

    public DataContextFactoryRegistryImpl() {
        factories = new ArrayList<>();
    }

    @Override
    public void addFactory(DataContextFactory factory) {
        factories.add(factory);
    }

    @Override
    public void clearFactories() {
        factories.clear();
    }

    public void discoverFromClasspath() {
        final ServiceLoader<DataContextFactory> serviceLoader = ServiceLoader.load(DataContextFactory.class);
        for (DataContextFactory factory : serviceLoader) {
            addFactory(factory);
        }
    }

    @Override
    public Collection<DataContextFactory> getFactories() {
        return Collections.unmodifiableList(factories);
    }

    @Override
    public DataContext createDataContext(DataContextProperties properties) throws UnsupportedDataContextPropertiesException {
        for (DataContextFactory factory : factories) {
            if (factory.accepts(properties)) {
                return factory.create(properties);
            }
        }
        throw new UnsupportedDataContextPropertiesException();
    }
}

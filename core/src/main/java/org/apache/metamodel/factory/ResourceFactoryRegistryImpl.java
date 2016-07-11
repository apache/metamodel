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

import org.apache.metamodel.util.Resource;

public class ResourceFactoryRegistryImpl implements ResourceFactoryRegistry {

    private static final ResourceFactoryRegistry DEFAULT_INSTANCE;

    static {
        final ResourceFactoryRegistryImpl registry = new ResourceFactoryRegistryImpl();
        registry.discoverFromClasspath();
        DEFAULT_INSTANCE = registry;
    }

    public static ResourceFactoryRegistry getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    private final List<ResourceFactory> factories;

    public ResourceFactoryRegistryImpl() {
        factories = new ArrayList<>();
    }

    @Override
    public void addFactory(ResourceFactory factory) {
        factories.add(factory);
    }

    @Override
    public void clearFactories() {
        factories.clear();
    }

    @Override
    public Collection<ResourceFactory> getFactories() {
        return Collections.unmodifiableList(factories);
    }

    @Override
    public Resource createResource(ResourceProperties properties) {
        for (ResourceFactory factory : factories) {
            if (factory.accepts(properties)) {
                return factory.create(properties);
            }
        }
        throw new UnsupportedResourcePropertiesException();
    }

    public void discoverFromClasspath() {
        final ServiceLoader<ResourceFactory> serviceLoader = ServiceLoader.load(ResourceFactory.class);
        for (ResourceFactory factory : serviceLoader) {
            addFactory(factory);
        }
    }
}

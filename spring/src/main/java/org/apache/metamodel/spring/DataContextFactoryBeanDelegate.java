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

import org.apache.metamodel.DataContext;

/**
 * Component responsible for instantiating a {@link DataContext} given the
 * properties of a {@link DataContextFactoryBean}.
 * 
 * This component helps separate the configuration code (in
 * {@link DataContextFactoryBean}) from the actual instantiation of objects.
 * This is necesary to ensure that the {@link DataContextFactoryBean} is
 * functional for only a subset of MetaModel's modules, since otherwise import
 * statements would hinder use of the class when not all MetaModel modules are
 * available.
 */
public interface DataContextFactoryBeanDelegate {

    /**
     * Creates the particular {@link DataContext} object.
     * 
     * @param params
     *            the {@link DataContextFactoryBean} with properties set as per
     *            the spring configuration.
     * @return a non-null {@link DataContext} object, built with the properties
     *         set on the {@link DataContextFactoryBean}
     */
    public DataContext createDataContext(DataContextFactoryParameters params);
}

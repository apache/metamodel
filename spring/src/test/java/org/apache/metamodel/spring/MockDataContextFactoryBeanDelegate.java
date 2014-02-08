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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.pojo.PojoDataContext;
import org.apache.metamodel.pojo.TableDataProvider;

/**
 * Mock implementation of {@link DataContextFactoryBeanDelegate}. Used by test (
 * {@link MockDataContextTypeTest}) to verify that we can pass data context type
 * as a qualified class name of a factory delegate.
 */
public class MockDataContextFactoryBeanDelegate implements DataContextFactoryBeanDelegate {

    @Override
    public DataContext createDataContext(DataContextFactoryParameters bean) {
        String username = bean.getUsername();
        return new PojoDataContext(username, new ArrayList<TableDataProvider<?>>());
    }

}

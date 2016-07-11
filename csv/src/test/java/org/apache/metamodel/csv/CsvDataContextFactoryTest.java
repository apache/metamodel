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
package org.apache.metamodel.csv;

import java.util.Collection;

import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.junit.Assert;
import org.junit.Test;

public class CsvDataContextFactoryTest {

    @Test
    public void testDiscovery() throws Exception {
        final Collection<DataContextFactory> factories = DataContextFactoryRegistryImpl.getDefaultInstance()
                .getFactories();

        boolean found = false;
        for (DataContextFactory factory : factories) {
            if (factory instanceof CsvDataContextFactory) {
                found = true;
                break;
            }
        }

        if (!found) {
            Assert.fail("Expected to find CsvDataContextFactory. Found: " + factories);
        }
    }

    @Test
    public void testCreateDataContext() throws Exception {
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.put("type", "csv");
        properties.put("resource", "src/test/resources/csv_people.csv");

        DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(properties);
    }
}

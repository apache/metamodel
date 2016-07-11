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
package org.apache.metamodel.jdbc;

import static org.junit.Assert.assertTrue;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactoryRegistryImpl;
import org.apache.metamodel.factory.DataContextPropertiesImpl;
import org.apache.metamodel.jdbc.dialects.H2QueryRewriter;
import org.junit.Test;

public class JdbcDataContextFactoryTest {

    @Test
    public void testCreateDataContext() throws Exception {
        final DataContextPropertiesImpl properties = new DataContextPropertiesImpl();
        properties.setDataContextType("jdbc");
        properties.put("driver-class", H2databaseTest.DRIVER_CLASS);
        properties.put("url", H2databaseTest.URL_MEMORY_DATABASE);

        final DataContext dataContext = DataContextFactoryRegistryImpl.getDefaultInstance().createDataContext(
                properties);
        assertTrue(dataContext instanceof JdbcDataContext);

        final JdbcDataContext jdbcDataContext = (JdbcDataContext) dataContext;
        assertTrue(jdbcDataContext.getQueryRewriter() instanceof H2QueryRewriter);
    }
}

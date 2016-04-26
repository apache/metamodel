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
package org.apache.metamodel.schema.naming;

import static org.junit.Assert.*;

import org.junit.Test;

public class DelegatingIntrinsicSwitchColumnNamingStrategyTest {

    private final ColumnNamingStrategy namingStrategy = ColumnNamingStrategies.defaultStrategy();
    
    @Test
    public void testItIsTheDefaultStrategy() throws Exception {
        assertTrue(namingStrategy instanceof DelegatingIntrinsicSwitchColumnNamingStrategy);
    }

    @Test
    public void testDuplicateColumnNames() throws Exception {
        try (final ColumnNamingSession session = namingStrategy.startColumnNamingSession()) {
            assertEquals("foo", session.getNextColumnName(new ColumnNamingContextImpl(null, "foo", 0)));
            assertEquals("bar", session.getNextColumnName(new ColumnNamingContextImpl(null, "bar", 1)));
            assertEquals("foo_2", session.getNextColumnName(new ColumnNamingContextImpl(null, "foo", 2)));
        }
    }

    @Test
    public void testNoIntrinsicColumnNames() throws Exception {
        try (final ColumnNamingSession session = namingStrategy.startColumnNamingSession()) {
            assertEquals("A", session.getNextColumnName(new ColumnNamingContextImpl(null, "", 0)));
            assertEquals("B", session.getNextColumnName(new ColumnNamingContextImpl(null, null, 1)));
            assertEquals("C", session.getNextColumnName(new ColumnNamingContextImpl(null, "", 2)));
        }
    }
}

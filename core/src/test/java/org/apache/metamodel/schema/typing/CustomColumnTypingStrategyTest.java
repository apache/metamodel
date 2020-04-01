/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.schema.typing;

import org.apache.metamodel.schema.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CustomColumnTypingStrategyTest {

    @Test
    public void testTypeConfiguration() throws Exception {
        ColumnTypingStrategy strategy = ColumnTypingStrategies.customTypes(ColumnType.STRING, ColumnType.NUMBER);
        try ( final ColumnTypingSession session = strategy.startColumnTypingSession() ) {
            assertEquals(ColumnType.STRING, session.getNextColumnType(new ColumnTypingContextImpl(0)));
            assertEquals(ColumnType.NUMBER, session.getNextColumnType(new ColumnTypingContextImpl(1)));
        }
    }
}

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
package org.eobjects.metamodel.query;

import junit.framework.TestCase;

public class OperatorTypeTest extends TestCase {

    public void testConvertOperatorType() throws Exception {
        assertEquals(OperatorType.EQUALS_TO, OperatorType.convertOperatorType("="));
        assertEquals(OperatorType.GREATER_THAN, OperatorType.convertOperatorType(">"));
        assertEquals(OperatorType.LESS_THAN, OperatorType.convertOperatorType("<"));
        assertEquals(OperatorType.DIFFERENT_FROM, OperatorType.convertOperatorType("<>"));
        assertEquals(OperatorType.LIKE, OperatorType.convertOperatorType("LIKE"));
        assertEquals(OperatorType.IN, OperatorType.convertOperatorType("IN"));
        assertEquals(null, OperatorType.convertOperatorType("foo"));
    }
}

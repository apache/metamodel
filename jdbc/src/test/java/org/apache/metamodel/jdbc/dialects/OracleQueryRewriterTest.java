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
package org.apache.metamodel.jdbc.dialects;

import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OracleQueryRewriterTest {

    @Test
    public void testReplaceEmptyStringWithNull() throws Exception {
        final OracleQueryRewriter rewriter = new OracleQueryRewriter(null);
        final String alias = "alias";
        SelectItem selectItem = new SelectItem("expression", alias);
        final FilterItem filterItem = new FilterItem(selectItem, OperatorType.DIFFERENT_FROM, "");
        final String rewrittenValue = rewriter.rewriteFilterItem(filterItem);
        final String expectedValue = alias + " IS NOT NULL";
        
        assertEquals(expectedValue, rewrittenValue);
    }
}
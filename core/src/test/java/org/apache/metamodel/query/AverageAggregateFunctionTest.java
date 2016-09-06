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
package org.apache.metamodel.query;

import static org.junit.Assert.assertEquals;

import org.apache.metamodel.MockDataContext;
import org.apache.metamodel.data.DataSet;
import org.junit.Test;

public class AverageAggregateFunctionTest {

    @Test
    public void testEvaluateAvgOfIntegersWithQueryPostprocessor() throws Exception {
        final MockDataContext dc = new MockDataContext("sch", "tbl", "foo");
        try (final DataSet ds = dc.query().from("tbl").select("AVG(foo)", "SUM(foo)", "COUNT(*)").execute()) {
            ds.next();
            assertEquals("Row[values=[2.5, 10.0, 4]]", ds.getRow().toString());
        }
    }
}

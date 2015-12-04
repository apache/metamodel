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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class RandomAggregateBuilderTest {

    @Test
    public void testOnlyOne() throws Exception {
        RandomAggregateBuilder aggregateBuilder = new RandomAggregateBuilder();
        aggregateBuilder.add("foo");
        assertEquals("foo", aggregateBuilder.getAggregate());
    }

    @Test
    public void testRandomBehaviour() throws Exception {
        // run 1000 tests with 3 values to ensure that there is at least an
        // approximate fair distribution of randomized results
        final int samples = 10000;
        final int minimumExpectation = samples / 5;

        final Map<String, AtomicInteger> counterMap = new HashMap<>();
        counterMap.put("foo", new AtomicInteger());
        counterMap.put("bar", new AtomicInteger());
        counterMap.put("baz", new AtomicInteger());

        for (int i = 0; i < samples; i++) {
            final RandomAggregateBuilder aggregateBuilder = new RandomAggregateBuilder();
            aggregateBuilder.add("foo");
            aggregateBuilder.add(null);
            aggregateBuilder.add("bar");
            aggregateBuilder.add("baz");

            counterMap.get(aggregateBuilder.getAggregate()).incrementAndGet();
        }

        final String messageString = "got: " + counterMap.toString();
        final Set<Entry<String, AtomicInteger>> entries = counterMap.entrySet();
        for (Entry<String, AtomicInteger> entry : entries) {
            final int count = entry.getValue().get();
            if (count < minimumExpectation) {
                assertEquals(messageString, minimumExpectation, count);
            }
        }
    }
}

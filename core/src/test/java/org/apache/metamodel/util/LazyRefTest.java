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
package org.apache.metamodel.util;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

public class LazyRefTest extends TestCase {

    public void testRequestLoad() throws Exception {
        LazyRef<Integer> lazyRef = new LazyRef<Integer>() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            protected Integer fetch() {
                return counter.incrementAndGet();
            }
        };

        lazyRef.requestLoad();
        Thread.sleep(20);
        Integer integer = lazyRef.get();
        assertEquals(1, integer.intValue());
    }

    public void testErrorHandling() throws Exception {
        LazyRef<Object> ref = new LazyRef<Object>() {
            @Override
            protected Object fetch() throws Throwable {
                throw new Exception("foo");
            }
        };

        assertNull(ref.get());
        assertEquals("foo", ref.getError().getMessage());

        // now with a runtime exception (retain previous behaviour in this
        // regard)
        ref = new LazyRef<Object>() {
            @Override
            protected Object fetch() throws Throwable {
                throw new IllegalStateException("bar");
            }
        };

        try {
            ref.get();
            fail("Exception expected");
        } catch (IllegalStateException e) {
            assertEquals("bar", e.getMessage());
        }
    }

    public void testGet() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        LazyRef<String> lazyRef = new LazyRef<String>() {
            @Override
            protected String fetch() {
                counter.incrementAndGet();
                return "foo";
            }
        };

        assertFalse(lazyRef.isFetched());
        assertEquals(0, counter.get());

        assertEquals("foo", lazyRef.get());
        assertEquals("foo", lazyRef.get());
        assertEquals("foo", lazyRef.get());

        assertTrue(lazyRef.isFetched());
        assertEquals(1, counter.get());
    }
}

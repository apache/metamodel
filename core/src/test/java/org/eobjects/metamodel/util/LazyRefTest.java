/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.util;

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

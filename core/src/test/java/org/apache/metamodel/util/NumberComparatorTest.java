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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.Comparator;

import org.junit.Test;

public class NumberComparatorTest {

    @Test
    public void testDoubleAndIntegerComparison() throws Exception {
        Comparator<Object> comparator = NumberComparator.getComparator();
        assertEquals(0, comparator.compare(1, 1.0));
    }

    @Test
    public void testComparable() throws Exception {
        Comparable<Object> comparable = NumberComparator.getComparable("125");
        assertEquals(0, comparable.compareTo(125));
        assertEquals(-1, comparable.compareTo(126));
    }

    @Test
    public void testToNumberInt() throws Exception {
        assertEquals(4212, NumberComparator.toNumber("4212"));
    }

    @Test
    public void testToNumberLong() throws Exception {
        assertEquals(4212000000l, NumberComparator.toNumber("4212000000"));
    }

    @Test
    public void testToNumberDouble() throws Exception {
        assertEquals(42.12, NumberComparator.toNumber("42.12"));
    }

    @Test
    public void testCompareNull() throws Exception {
        assertTrue(NumberComparator.getComparator().compare(null, null) == 0);
        assertTrue(NumberComparator.getComparator().compare(null, "1234") < 0);
        assertTrue(NumberComparator.getComparator().compare("1234", null) > 0);
    }

    @Test
    public void testCompareBoolean() throws Exception {
        assertTrue(NumberComparator.getComparator().compare("1", "true") == 0);
        assertTrue(NumberComparator.getComparator().compare("1", "false") > 0);
        assertTrue(NumberComparator.getComparator().compare("0", "false") == 0);
    }

    @Test
    public void testCompareOneNonConvertableStrings() throws Exception {
        assertTrue(NumberComparator.getComparator().compare("1", "bar") > 0);
        assertTrue(NumberComparator.getComparator().compare("foo", "2") < 0);
    }
    
    @Test
    public void testCompareBothNonConvertableStrings() throws Exception {
        // odd cases we don't support - but for regression here's some
        // "documentation" of it's behaviour
        assertTrue(NumberComparator.getComparator().compare("foo", null) == 0);
        assertTrue(NumberComparator.getComparator().compare("foo", "bar") == 0);
        assertTrue(NumberComparator.getComparator().compare("foo", "") == 0);
        assertTrue(NumberComparator.getComparator().compare(null, "bar") == 0);
    }

    @Test
    public void testCompareBigIntegers() throws Exception {
        assertTrue(NumberComparator.getComparator().compare(BigInteger.ONE, BigInteger.ONE) == 0);
        assertTrue(NumberComparator.getComparator().compare(BigInteger.ONE, BigInteger.TEN) < 0);

        assertTrue(NumberComparator.getComparator().compare(BigInteger.ONE, BigInteger.ONE) == 0);
        assertTrue(NumberComparator.getComparator().compare(new BigInteger("-4239842739427492"),
                new BigInteger("-3239842739427492")) < 0);
    }

    @Test
    public void testCompareBigDecimals() throws Exception {
        assertTrue(NumberComparator.getComparator().compare(BigInteger.ONE, BigInteger.ONE) == 0);
        assertTrue(NumberComparator.getComparator().compare(BigInteger.ONE, BigInteger.TEN) < 0);

        assertTrue(NumberComparator.getComparator().compare(BigInteger.ONE, BigInteger.ONE) == 0);
        assertTrue(NumberComparator.getComparator().compare(new BigInteger("-4239842739427492"),
                new BigInteger("-3239842739427492")) < 0);
    }

    @Test
    public void testCompareIntegers() throws Exception {
        assertTrue(NumberComparator.getComparator().compare(42, 42) == 0);
        assertTrue(NumberComparator.getComparator().compare(42, 43) < 0);
    }

    @Test
    public void testCompareLongs() throws Exception {
        assertTrue(NumberComparator.getComparator().compare(42000000000l, 42000000000l) == 0);
        assertTrue(NumberComparator.getComparator().compare(42000000000l, 42000000001l) < 0);
    }

    @Test
    public void testCompareDoubles() throws Exception {
        assertTrue(NumberComparator.getComparator().compare(42.01, 42.01) == 0);
        assertTrue(NumberComparator.getComparator().compare(42.01, 42.02) < 0);
    }
}
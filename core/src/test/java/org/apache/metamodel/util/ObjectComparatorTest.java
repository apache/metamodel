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

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

import junit.framework.TestCase;

public class ObjectComparatorTest extends TestCase {

	public void testString() throws Exception {
		Comparator<Object> c = ObjectComparator.getComparator();
		assertTrue(c.compare("aaaa", "bbbb") < 0);

		assertTrue(c.compare("w", "y") < 0);
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = ObjectComparator.getComparable("aaaa");
		assertEquals(-1, comparable.compareTo("bbbb"));
	}

	public void testNull() throws Exception {
		Comparator<Object> comparator = ObjectComparator.getComparator();
		assertEquals(0, comparator.compare(null, null));
		assertEquals(1, comparator.compare("h", null));
		assertEquals(-1, comparator.compare(null, "h"));

		TreeSet<Object> set = new TreeSet<Object>(comparator);
		set.add("Hello");
		set.add(null);
		set.add(null);
		set.add(DateUtils.get(2010, Month.SEPTEMBER, 27));
		set.add(DateUtils.get(2010, Month.SEPTEMBER, 28));
		set.add(DateUtils.get(2010, Month.SEPTEMBER, 26));

		assertEquals(5, set.size());
		Iterator<Object> it = set.iterator();
		assertEquals(null, it.next());
		assertEquals("Hello", it.next());
		assertEquals(DateUtils.get(2010, Month.SEPTEMBER, 26), it.next());
		assertEquals(DateUtils.get(2010, Month.SEPTEMBER, 27), it.next());
		assertEquals(DateUtils.get(2010, Month.SEPTEMBER, 28), it.next());
	}
}
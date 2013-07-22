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

import junit.framework.TestCase;
import org.apache.metamodel.util.ToStringComparator;

public class ToStringComparatorTest extends TestCase {

	private Comparator<Object> comparator = ToStringComparator.getComparator();

	public void testNotNull() throws Exception {
		assertEquals(4, comparator.compare("foo", "bar"));
		assertEquals(-4, comparator.compare("bar", "foo"));
	}

	public void testNull() throws Exception {
		int result = comparator.compare(null, null);
		assertEquals(-1, result);

		result = comparator.compare(1, null);
		assertEquals(1, result);

		result = comparator.compare(null, 1);
		assertEquals(-1, result);
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = ToStringComparator
				.getComparable("aaaa");
		assertEquals(-1, comparable.compareTo("bbbb"));
	}
}
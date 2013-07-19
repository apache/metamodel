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

public class BooleanComparatorTest extends TestCase {

	public void testCompare() throws Exception {
		Comparator<Object> c = BooleanComparator.getComparator();
		assertEquals(1, c.compare(true, false));
		assertEquals(-1, c.compare(false, true));
		assertEquals(0, c.compare(true, true));
		assertEquals(0, c.compare(false, false));

		assertEquals(1, c.compare("true", "false"));
		assertEquals(1, c.compare("1", "false"));
		assertEquals(1, c.compare("true", "0"));
		assertEquals(1, c.compare("true", "false"));

		assertEquals(1, c.compare(1, 0));

		assertEquals(1, c.compare(1, "false"));
		assertEquals(1, c.compare("yes", false));
		assertEquals(1, c.compare("y", false));
		assertEquals(1, c.compare("TRUE", false));
	}

	public void testComparable() throws Exception {
		Comparable<Object> comparable = BooleanComparator.getComparable(true);
		assertEquals(1, comparable.compareTo(false));
		assertEquals(1, comparable.compareTo(0));
		assertEquals(1, comparable.compareTo("false"));
	}
}
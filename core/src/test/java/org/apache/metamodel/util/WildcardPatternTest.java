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

import junit.framework.TestCase;

public class WildcardPatternTest extends TestCase {

	public void testMatches() throws Exception {
		WildcardPattern pattern = new WildcardPattern("foo%bar", '%');
		assertTrue(pattern.matches("foobar"));
		assertTrue(pattern.matches("foofoobar"));
		assertFalse(pattern.matches("foobarbar"));
		assertFalse(pattern.matches("w00p"));

		pattern = new WildcardPattern("*foo*bar", '*');
		assertTrue(pattern.matches("foobar"));
		assertTrue(pattern.matches("foofoobar"));
		assertFalse(pattern.matches("foobarbar"));
		assertFalse(pattern.matches("w00p"));

		pattern = new WildcardPattern("foo%bar%", '%');
		assertTrue(pattern.matches("foobar"));
		assertTrue(pattern.matches("foofoobar"));
		assertTrue(pattern.matches("foobarbar"));
		assertFalse(pattern.matches("w00p"));

		pattern = new WildcardPattern("oba%", '%');
		assertTrue(pattern.matches("obar"));
		assertFalse(pattern.matches("foobar"));

		pattern = new WildcardPattern("bar", '%');
		assertTrue(pattern.matches("bar"));
		assertFalse(pattern.matches("foobar"));

		pattern = new WildcardPattern("", '%');
		assertTrue(pattern.matches(""));
		assertFalse(pattern.matches("foo"));
	}
}
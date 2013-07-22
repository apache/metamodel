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

import junit.framework.TestCase;

public class FunctionTypeTest extends TestCase {

	public void testEvaluateNumbers() throws Exception {
		assertEquals(2.5, FunctionType.AVG.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(10.0, FunctionType.SUM.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(4l, FunctionType.COUNT.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(1.5, FunctionType.MIN.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(3.5, FunctionType.MAX.evaluate(1.5, 2, null, 3, 3.5));
	}

	public void testEvaluateStrings() throws Exception {
		assertEquals(2.5, FunctionType.AVG.evaluate("1.5", "2", null, "3",
				"3.5"));
		assertEquals(10.0, FunctionType.SUM.evaluate("1.5", "2", null, "3",
				"3.5"));
		assertEquals(2l, FunctionType.COUNT.evaluate("foo", "BAR", null));
		assertEquals("a", FunctionType.MIN.evaluate("abc", "a", null, "bcd"));
		assertEquals("bcd", FunctionType.MAX.evaluate("abc", "a", null, "bcd"));
	}
}
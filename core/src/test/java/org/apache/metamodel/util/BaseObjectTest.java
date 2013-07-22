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

import java.util.List;

import junit.framework.TestCase;

public class BaseObjectTest extends TestCase {

	class MyClass extends BaseObject {
		private int[] ints;

		@Override
		protected void decorateIdentity(List<Object> identifiers) {
			identifiers.add(ints);
		}
	}

	public void testHashCodeForPrimitiveArray() throws Exception {
		MyClass o1 = new MyClass();
		o1.ints = new int[] { 1, 2, 3 };
		MyClass o2 = new MyClass();
		o2.ints = new int[] { 4, 5, 6 };
		MyClass o3 = new MyClass();
		o3.ints = new int[] { 1, 2, 3 };

		assertTrue(o1.hashCode() == o1.hashCode());
		assertTrue(o1.hashCode() == o3.hashCode());
		assertFalse(o1.hashCode() == o2.hashCode());
		assertFalse(o3.hashCode() == o2.hashCode());
	}
}

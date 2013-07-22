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
package org.apache.metamodel.convert;

import junit.framework.TestCase;

public class StringToIntegerConverterTest extends TestCase {

	private StringToIntegerConverter conv = new StringToIntegerConverter();

	public void testToVirtual() throws Exception {
		assertNull(conv.toVirtualValue(null));
		assertNull(conv.toVirtualValue(""));
		assertEquals(123, conv.toVirtualValue("123").intValue());
	}

	public void testToPhysical() throws Exception {
		assertNull(conv.toPhysicalValue(null));
		assertEquals("123", conv.toPhysicalValue(123));
	}
}

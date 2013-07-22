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

public class ColumnTypeDetectorTest extends TestCase {

	public void testBooleanConverter() throws Exception {
		ColumnTypeDetector d = new ColumnTypeDetector();

		d.registerValue("1");
		d.registerValue("true");
		d.registerValue("0");

		assertEquals(StringToBooleanConverter.class, d.createConverter()
				.getClass());

		d.registerValue("2");

		assertNull(d.createConverter());
	}

	public void testIntegerAndDoubleConverter() throws Exception {
		ColumnTypeDetector d = new ColumnTypeDetector();

		d.registerValue("123");
		d.registerValue("0");

		assertEquals(StringToIntegerConverter.class, d.createConverter()
				.getClass());

		d.registerValue("1123.23");
		d.registerValue("0.0");

		assertEquals(StringToDoubleConverter.class, d.createConverter()
				.getClass());

		d.registerValue("abc");

		assertNull(d.createConverter());
	}

	public void testDateConverter() throws Exception {
		ColumnTypeDetector d = new ColumnTypeDetector();

		d.registerValue("2010-12-30");

		assertEquals(StringToDateConverter.class, d.createConverter()
				.getClass());

		d.registerValue("2 abc");

		assertNull(d.createConverter());
	}
}

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
package org.apache.metamodel.schema;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import junit.framework.TestCase;

public class JavaTypesTest extends TestCase {

	/**
	 * Tests that the constant values of java 6 is backwards compatible with
	 * java 5
	 */
	public void testConstantValues() throws Exception {
		Class<Java5Types> types5 = Java5Types.class;
		Class<JdbcTypes> types6 = JdbcTypes.class;
		Field[] fields = types5.getFields();
		for (int i = 0; i < fields.length; i++) {
			Field field5 = fields[i];
			String fieldName = field5.getName();
			int mod = field5.getModifiers();
			if (Modifier.isFinal(mod) && Modifier.isPublic(mod)
					&& Modifier.isStatic(mod)) {
				int value5 = field5.getInt(null);
				Field field6 = types6.getField(fieldName);
				int value6 = field6.getInt(null);
				assertEquals("Value of field " + fieldName
						+ " was not the same", value5, value6);
			}
		}
	}
}
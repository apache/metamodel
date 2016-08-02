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
package org.apache.metamodel.fixedwidth;

import junit.framework.TestCase;

public class FixedWidthConfigurationTest extends TestCase {

	public void testToString() throws Exception {
		assertEquals(
				"FixedWidthConfiguration[encoding=UTF8, fixedValueWidth=10, valueWidths=[], columnNameLineNumber=1, failOnInconsistentLineWidth=true]",
				new FixedWidthConfiguration(1, "UTF8", 10, true).toString());
	}

	public void testEquals() throws Exception {
		FixedWidthConfiguration conf1 = new FixedWidthConfiguration(1, "UTF8", 10, true);
		FixedWidthConfiguration conf2 = new FixedWidthConfiguration(1, "UTF8", 10, true);
		assertEquals(conf1, conf2);

		FixedWidthConfiguration conf3 = new FixedWidthConfiguration(1, "UTF8", 10, false);
		assertFalse(conf1.equals(conf3));
	}
}

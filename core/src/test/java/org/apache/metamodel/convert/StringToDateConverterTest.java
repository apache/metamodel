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

import java.text.DateFormat;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.metamodel.util.DateUtils;
import org.apache.metamodel.util.Month;

public class StringToDateConverterTest extends TestCase {

	public void testToVirtualSimpleDateFormat() throws Exception {
		StringToDateConverter conv = new StringToDateConverter("yyyy-MM-dd");
		assertNull(conv.toVirtualValue(null));
		assertNull(conv.toVirtualValue(""));

		assertEquals(DateUtils.get(2010, Month.DECEMBER, 31),
				conv.toVirtualValue("2010-12-31"));
	}

	public void testToVirtualNoArgs() throws Exception {
		StringToDateConverter conv = new StringToDateConverter();
		assertNull(conv.toVirtualValue(null));
		assertNull(conv.toVirtualValue(""));

		assertEquals(DateUtils.get(2010, Month.DECEMBER, 31),
				conv.toVirtualValue("2010-12-31"));
	}

	public void testToPhysicalSimpleDateFormat() throws Exception {
		StringToDateConverter conv = new StringToDateConverter("yyyy-MM-dd");
		assertNull(conv.toPhysicalValue(null));
		Date input = DateUtils.get(2010, Month.DECEMBER, 31);
		String physicalValue = conv.toPhysicalValue(input);
		assertEquals("2010-12-31", physicalValue);
	}

	public void testToPhysicalNoArgs() throws Exception {
		StringToDateConverter conv = new StringToDateConverter();
		assertNull(conv.toPhysicalValue(null));
		Date input = DateUtils.get(2010, Month.DECEMBER, 31);
		String physicalValue = conv.toPhysicalValue(input);
		Date virtualValue = DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
				DateFormat.MEDIUM).parse(physicalValue);
		assertEquals(virtualValue, input);
	}
}

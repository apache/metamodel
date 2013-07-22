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

import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;

public class DateUtilsTest extends TestCase {

	public void testGet() throws Exception {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Date christmasDay = DateUtils.get(2010, Month.DECEMBER, 24);
		assertEquals("2010-12-24 00:00:00", f.format(christmasDay));
		assertEquals(Weekday.FRIDAY, DateUtils.getWeekday(christmasDay));
		
		Date date2 = DateUtils.get(christmasDay, 1);
		assertEquals("2010-12-25 00:00:00", f.format(date2));
		
		Date date3 = DateUtils.get(christmasDay, 10);
		assertEquals("2011-01-03 00:00:00", f.format(date3));
	}
}

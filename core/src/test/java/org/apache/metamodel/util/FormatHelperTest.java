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

import java.text.NumberFormat;
import java.util.Arrays;

import org.apache.metamodel.schema.ColumnType;

import junit.framework.TestCase;

public class FormatHelperTest extends TestCase {

	public void testNumberFormat() throws Exception {
		NumberFormat format = FormatHelper.getUiNumberFormat();
		assertEquals("987643.213456343", format.format(987643.213456343));
		assertEquals("0.218456343", format.format(0.218456343));
		assertEquals("20.1", format.format(20.1));
	}

	public void testFormatSqlValue() throws Exception {
		assertEquals("'foo'", FormatHelper.formatSqlValue(null, "foo"));
		assertEquals("1", FormatHelper.formatSqlValue(null, 1));
		assertEquals("NULL", FormatHelper.formatSqlValue(null, null));
		assertEquals(
				"TIMESTAMP '2011-07-24 00:00:00'",
				FormatHelper.formatSqlValue(ColumnType.TIMESTAMP,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"DATE '2011-07-24'",
				FormatHelper.formatSqlValue(ColumnType.DATE,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"TIME '00:00:00'",
				FormatHelper.formatSqlValue(ColumnType.TIME,
						DateUtils.get(2011, Month.JULY, 24)));
		assertEquals(
				"('foo' , 1 , 'bar' , 0.1234)",
				FormatHelper.formatSqlValue(null,
						Arrays.asList("foo", 1, "bar", 0.1234)));
		assertEquals(
				"('foo' , 1 , 'bar' , 0.1234)",
				FormatHelper.formatSqlValue(null, new Object[] { "foo", 1,
						"bar", 0.1234 }));
	}
}
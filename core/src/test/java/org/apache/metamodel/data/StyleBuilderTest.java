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
package org.apache.metamodel.data;

import org.apache.metamodel.data.Style.Color;

import junit.framework.TestCase;

public class StyleBuilderTest extends TestCase {

	public void testDefaultColors() throws Exception {
		StyleBuilder sb = new StyleBuilder();

		sb.foreground(1, 1, 1);
		assertEquals("color: rgb(1,1,1);", sb.create().toCSS());

		sb.foreground(0, 0, 0);
		assertEquals("", sb.create().toCSS());

		sb.background(0, 0, 0);
		assertEquals("background-color: rgb(0,0,0);", sb.create().toCSS());

		sb.background(255, 255, 255);
		assertEquals("", sb.create().toCSS());
	}

	public void testCreateNoStyle() throws Exception {
		Style style = new StyleBuilder().create();
		assertEquals(Style.NO_STYLE, style);
		assertSame(Style.NO_STYLE, style);
	}

	public void testCreateColor() throws Exception {
		Color col1 = StyleBuilder.createColor("eeEE00");
		assertEquals("Color[238,238,0]", col1.toString());

		Color col2 = StyleBuilder.createColor(238, 238, 0);

		// cache should ensure that these two colors are not only equal, but
		// also the same!
		assertEquals(col1, col2);
		assertSame(col1, col2);
	}
}

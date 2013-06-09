/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.data;

import org.eobjects.metamodel.data.Style.Color;

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

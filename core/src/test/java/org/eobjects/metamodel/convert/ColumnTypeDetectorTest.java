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
package org.eobjects.metamodel.convert;

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

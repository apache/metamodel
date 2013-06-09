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

package org.eobjects.metamodel.query;

import junit.framework.TestCase;

public class FunctionTypeTest extends TestCase {

	public void testEvaluateNumbers() throws Exception {
		assertEquals(2.5, FunctionType.AVG.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(10.0, FunctionType.SUM.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(4l, FunctionType.COUNT.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(1.5, FunctionType.MIN.evaluate(1.5, 2, null, 3, 3.5));
		assertEquals(3.5, FunctionType.MAX.evaluate(1.5, 2, null, 3, 3.5));
	}

	public void testEvaluateStrings() throws Exception {
		assertEquals(2.5, FunctionType.AVG.evaluate("1.5", "2", null, "3",
				"3.5"));
		assertEquals(10.0, FunctionType.SUM.evaluate("1.5", "2", null, "3",
				"3.5"));
		assertEquals(2l, FunctionType.COUNT.evaluate("foo", "BAR", null));
		assertEquals("a", FunctionType.MIN.evaluate("abc", "a", null, "bcd"));
		assertEquals("bcd", FunctionType.MAX.evaluate("abc", "a", null, "bcd"));
	}
}
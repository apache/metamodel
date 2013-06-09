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

package org.eobjects.metamodel.schema;

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
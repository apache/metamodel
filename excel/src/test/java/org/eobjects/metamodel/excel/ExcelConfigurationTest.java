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
package org.eobjects.metamodel.excel;

import org.eobjects.metamodel.excel.ExcelConfiguration;

import junit.framework.TestCase;

public class ExcelConfigurationTest extends TestCase {

	public void testToString() throws Exception {
		ExcelConfiguration conf = new ExcelConfiguration(1, true, false);
		assertEquals(
				"ExcelConfiguration[columnNameLineNumber=1, skipEmptyLines=true, skipEmptyColumns=false]",
				conf.toString());
	}

	public void testEquals() throws Exception {
		ExcelConfiguration conf1 = new ExcelConfiguration(1, true, false);
		ExcelConfiguration conf2 = new ExcelConfiguration(1, true, false);
		ExcelConfiguration conf3 = new ExcelConfiguration(2, true, false);

		assertEquals(conf1, conf2);
		assertFalse(conf1.equals(conf3));
	}
}

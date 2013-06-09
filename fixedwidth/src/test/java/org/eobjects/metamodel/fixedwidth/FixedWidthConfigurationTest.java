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
package org.eobjects.metamodel.fixedwidth;

import org.eobjects.metamodel.fixedwidth.FixedWidthConfiguration;

import junit.framework.TestCase;

public class FixedWidthConfigurationTest extends TestCase {

	public void testToString() throws Exception {
		assertEquals(
				"FixedWidthConfiguration[encoding=UTF8, fixedValueWidth=10, valueWidths=[], columnNameLineNumber=1, failOnInconsistentLineWidth=true]",
				new FixedWidthConfiguration(1, "UTF8", 10, true).toString());
	}

	public void testEquals() throws Exception {
		FixedWidthConfiguration conf1 = new FixedWidthConfiguration(1, "UTF8",
				10, true);
		FixedWidthConfiguration conf2 = new FixedWidthConfiguration(1, "UTF8",
				10, true);
		assertEquals(conf1, conf2);

		FixedWidthConfiguration conf3 = new FixedWidthConfiguration(1, "UTF8",
				10, false);
		assertFalse(conf1.equals(conf3));
	}
}

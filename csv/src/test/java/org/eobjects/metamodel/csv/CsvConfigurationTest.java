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
package org.eobjects.metamodel.csv;

import junit.framework.TestCase;

public class CsvConfigurationTest extends TestCase {

	public void testToString() throws Exception {
		CsvConfiguration conf = new CsvConfiguration(0, "UTF8", ',', '"', '\\',
				true);
		assertEquals(
				"CsvConfiguration[columnNameLineNumber=0, encoding=UTF8, separatorChar=,, quoteChar=\", escapeChar=\\, failOnInconsistentRowLength=true]",
				conf.toString());
	}

	public void testEquals() throws Exception {
		CsvConfiguration conf1 = new CsvConfiguration(0, "UTF8", ',', '"',
				'\\', true);
		CsvConfiguration conf2 = new CsvConfiguration(0, "UTF8", ',', '"',
				'\\', true);

		assertEquals(conf1, conf2);

		CsvConfiguration conf3 = new CsvConfiguration(1, "UTF8", ',', '"',
				'\\', true);
		assertFalse(conf1.equals(conf3));
	}

}
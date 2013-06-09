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
package org.eobjects.metamodel.xml;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase;

import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.data.RowPublisher;
import org.eobjects.metamodel.data.Style;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

public class XmlSaxContentHandlerTest extends TestCase {

	public void testRun() throws Exception {
		final List<Object[]> rows = new ArrayList<Object[]>();

		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser = saxFactory.newSAXParser();
		XMLReader sheetParser = saxParser.getXMLReader();
		RowPublisher rowPublisher = new RowPublisher() {
			@Override
			public boolean publish(Object[] values, Style[] styles) {
				throw new UnsupportedOperationException();
			}

			@Override
			public boolean publish(Object[] values) {
				rows.add(values);
				return true;
			}

			@Override
			public boolean publish(Row row) {
				throw new UnsupportedOperationException();
			}

			@Override
			public void finished() {
			}
		};
		sheetParser.setContentHandler(new XmlSaxContentHandler(
				"/eobjects.dk/contributors/person", rowPublisher,
				"/eobjects.dk/contributors/person/name",
				"/eobjects.dk/contributors/person/address"));
		sheetParser.parse(new InputSource(new FileReader(
				"src/test/resources/xml_input_eobjects.xml")));

		assertEquals("[kasper, A third address]", Arrays.toString(rows.get(0)));
		assertEquals("[asbjorn, Asbjorns address]", Arrays.toString(rows.get(1)));
		assertEquals(2, rows.size());
	}
}

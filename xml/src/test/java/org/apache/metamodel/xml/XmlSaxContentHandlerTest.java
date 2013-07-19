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
package org.apache.metamodel.xml;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.RowPublisher;
import org.apache.metamodel.data.Style;
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

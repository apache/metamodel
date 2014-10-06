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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.metamodel.data.RowPublisher;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX handler for publishing records based on xpath expressions.
 */
class XmlSaxContentHandler extends DefaultHandler {

	private final boolean _checkAttributes;
	private final String _rowXpath;
	private final String[] _valueXpaths;
	private final StringBuilder _pathBuilder;
	private final StringBuilder _valueBuilder;
	private final RowPublisher _rowPublisher;
	private final Map<String, Integer> _indexIndexes;
	private final Map<String, AtomicInteger> _indexCounters;
	private volatile Object[] _rowValues;
	private volatile int _xpathIndex;

	public XmlSaxContentHandler(String rowXpath, RowPublisher rowPublisher,
			String... valueXpaths) {
		_indexCounters = new HashMap<String, AtomicInteger>();
		_indexIndexes = new HashMap<String, Integer>();
		_rowXpath = rowXpath;
		_rowPublisher = rowPublisher;
		_valueXpaths = valueXpaths;
		_rowValues = new Object[valueXpaths.length];
		boolean checkAttributes = false;
		for (int i = 0; i < valueXpaths.length; i++) {
			String xpath = valueXpaths[i];

			if (XmlSaxDataContext.COLUMN_NAME_ROW_ID.equals(xpath)) {
				// we use the indexing mechanism also for the row id.
				xpath = "index(" + xpath + ")";
			}

			if (xpath.startsWith("index(") && xpath.endsWith(")")) {
				xpath = xpath.substring("index(".length(), xpath.length() - 1);
				_indexCounters.put(xpath, new AtomicInteger(-1));
				_indexIndexes.put(xpath, i);

			} else if (xpath.indexOf('@') != -1) {
				checkAttributes = true;
			}
		}
		_checkAttributes = checkAttributes;
		_xpathIndex = -1;
		_pathBuilder = new StringBuilder();
		_valueBuilder = new StringBuilder();
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		_pathBuilder.append('/');
		_pathBuilder.append(qName);

		if (_checkAttributes) {
			for (int i = 0; i < attributes.getLength(); i++) {
				String attributeName = attributes.getQName(i);
				String attributeValue = attributes.getValue(i);
				startAttribute(attributeName, attributeValue);
				endAttribute(attributeName, attributeValue);
			}
		}

		String xpath = _pathBuilder.toString();

		AtomicInteger indexCounter = _indexCounters.get(xpath);
		if (indexCounter != null) {
			indexCounter.incrementAndGet();
		}

		_xpathIndex = indexOfXpath(xpath);
	}

	private int indexOfXpath(String path) {
		if (path == null || path.length() == 0) {
			return -1;
		}
		for (int i = 0; i < _valueXpaths.length; i++) {
			if (path.equals(_valueXpaths[i])) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public void characters(char[] ch, int start, int length)
			throws SAXException {
		if (_xpathIndex != -1) {
			_valueBuilder.append(ch, start, length);
		}
	}

	private void startAttribute(String attributeName, String attributeValue) {
		_pathBuilder.append('@');
		_pathBuilder.append(attributeName);

		int indexOfXpath = indexOfXpath(_pathBuilder.toString());
		if (indexOfXpath != -1) {
			_rowValues[indexOfXpath] = attributeValue;
		}
	}

	private void endAttribute(String attributeName, String attributeValue) {
		_pathBuilder.setLength(_pathBuilder.length() - attributeName.length()
				- 1);
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (_xpathIndex != -1) {
			_rowValues[_xpathIndex] = _valueBuilder.toString().trim();
		}
		_xpathIndex = -1;
		_valueBuilder.setLength(0);

		if (_rowXpath.equals(_pathBuilder.toString())) {
			insertRowIndexes();

			boolean more = _rowPublisher.publish(_rowValues);
			if (!more) {
				throw new XmlStopParsingException();
			}
			_rowValues = new Object[_valueXpaths.length];
		}

		_pathBuilder.setLength(_pathBuilder.length() - qName.length() - 1);
	}

	private void insertRowIndexes() {
		Set<Entry<String, Integer>> entrySet = _indexIndexes.entrySet();
		for (Entry<String, Integer> entry : entrySet) {
			String xpath = entry.getKey();
			Integer indexIndex = entry.getValue();
			AtomicInteger indexCount = _indexCounters.get(xpath);
			_rowValues[indexIndex] = indexCount.get();
		}
	}
}

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
package org.apache.metamodel.excel;

import java.util.Map;

import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.schema.TableType;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * XML handler for transforming a workbook document into {@link Table}s in a
 * MetaModel {@link Schema}.
 */
final class XlsxWorkbookToTablesHandler extends DefaultHandler {

	private final MutableSchema _schema;
	private final Map<String, String> _tableNamesToRelationshipIds;

	public XlsxWorkbookToTablesHandler(MutableSchema schema,
			Map<String, String> tableNamesToRelationshipIds) {
		_schema = schema;
		_tableNamesToRelationshipIds = tableNamesToRelationshipIds;
	}

	@Override
	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {
		if ("sheet".equals(qName)) {
			String name = attributes.getValue("name");
			assert name != null;
			String relationId = attributes.getValue("r:id");
			assert relationId != null;

			if (_schema != null) {
				MutableTable table = new MutableTable(name, TableType.TABLE,
						_schema);
				_schema.addTable(table);
			}
			_tableNamesToRelationshipIds.put(name, relationId);
		}
	}
}

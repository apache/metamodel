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

import java.util.Map;

import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.MutableTable;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.schema.TableType;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * XML handler for transforming a workbook document into {@link Table}s in a
 * MetaModel {@link Schema}.
 * 
 * @author Kasper Sørensen
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

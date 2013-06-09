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
package org.eobjects.metamodel.sugarcrm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eobjects.metamodel.schema.AbstractTable;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.Relationship;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.util.LazyRef;
import org.eobjects.metamodel.util.Ref;
import org.w3c.dom.Node;

import com.sugarcrm.ws.soap.FieldList;
import com.sugarcrm.ws.soap.NewModuleFields;
import com.sugarcrm.ws.soap.SelectFields;
import com.sugarcrm.ws.soap.SugarsoapPortType;

final class SugarCrmTable extends AbstractTable {

    private static final long serialVersionUID = 1L;

    private static final Map<String, ColumnType> TYPE_MAPPING;
    static {
        TYPE_MAPPING = new HashMap<String, ColumnType>();
        // known string types
        TYPE_MAPPING.put("name", ColumnType.VARCHAR);
        TYPE_MAPPING.put("assigned_user_name", ColumnType.VARCHAR);
        TYPE_MAPPING.put("text", ColumnType.VARCHAR);
        TYPE_MAPPING.put("enum", ColumnType.VARCHAR);
        TYPE_MAPPING.put("varchar", ColumnType.VARCHAR);
        TYPE_MAPPING.put("phone", ColumnType.VARCHAR);
        TYPE_MAPPING.put("fullname", ColumnType.VARCHAR);
        TYPE_MAPPING.put("url", ColumnType.VARCHAR);
        TYPE_MAPPING.put("relate", ColumnType.VARCHAR);
        TYPE_MAPPING.put("email", ColumnType.VARCHAR);
        TYPE_MAPPING.put("parent", ColumnType.VARCHAR);
        TYPE_MAPPING.put("parent_type", ColumnType.VARCHAR);
        TYPE_MAPPING.put("currency", ColumnType.VARCHAR);
        TYPE_MAPPING.put("none", ColumnType.VARCHAR);
        TYPE_MAPPING.put("user_name", ColumnType.VARCHAR);
        TYPE_MAPPING.put("file", ColumnType.VARCHAR);
        TYPE_MAPPING.put("id", ColumnType.VARCHAR);
        
        // known numbers
        TYPE_MAPPING.put("int", ColumnType.INTEGER);
        
        // known booleans
        TYPE_MAPPING.put("bool", ColumnType.BOOLEAN);
        
        // known timebased
        TYPE_MAPPING.put("date", ColumnType.DATE);
        TYPE_MAPPING.put("datetime", ColumnType.DATE);
        TYPE_MAPPING.put("datetimecombo", ColumnType.DATE);
    }

    private final String _name;
    private final Schema _schema;
    private final Ref<List<Column>> _columnsRef;

    public SugarCrmTable(String name, Schema schema, final SugarsoapPortType service, final Ref<String> sessionId) {
        _name = name;
        _schema = schema;
        _columnsRef = new LazyRef<List<Column>>() {

            @Override
            protected List<Column> fetch() {
                final List<Column> result = new ArrayList<Column>();
                final String session = sessionId.get();
                final NewModuleFields fields = service.getModuleFields(session, _name, new SelectFields());
                final FieldList moduleFields = fields.getModuleFields();
                final List<Object> list = moduleFields.getAny();

                for (Object object : list) {
                    if (object instanceof Node) {
                        final Node node = (Node) object;

                        final String name = SugarCrmXmlHelper.getChildElementText(node, "name");
                        final String nativeType = SugarCrmXmlHelper.getChildElementText(node, "type");
                        final String remarks = SugarCrmXmlHelper.getChildElementText(node, "label");
                        final ColumnType columnType = convertToColumnType(nativeType);

                        final Column column = new MutableColumn(name, columnType).setNativeType(nativeType)
                                .setRemarks(remarks).setTable(SugarCrmTable.this);
                        result.add(column);
                    }
                }
                return result;
            }
        };
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Column[] getColumns() {
        final List<Column> columns = _columnsRef.get();
        return columns.toArray(new Column[columns.size()]);
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    @Override
    public TableType getType() {
        return TableType.TABLE;
    }

    @Override
    public Relationship[] getRelationships() {
        return new Relationship[0];
    }

    @Override
    public String getRemarks() {
        return null;
    }

    @Override
    public String getQuote() {
        return null;
    }

    private ColumnType convertToColumnType(String sugarType) {
        ColumnType columnType = TYPE_MAPPING.get(sugarType);
        if (columnType == null) {
            columnType = ColumnType.OTHER;
        }
        return columnType;
    }
}

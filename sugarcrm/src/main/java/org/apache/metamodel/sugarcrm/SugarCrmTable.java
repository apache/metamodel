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
package org.apache.metamodel.sugarcrm;

import java.util.*;
import java.util.function.Supplier;

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.LazyRef;
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
    private final Supplier<List<Column>> _columnsRef;

    public SugarCrmTable(String name, Schema schema, final SugarsoapPortType service, final Supplier<String> sessionId) {
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
    public List<Column> getColumns() {
        final List<Column> columns = _columnsRef.get();
        return Collections.unmodifiableList(columns);
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
    public List<Relationship> getRelationships() {
        return new ArrayList<>();
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

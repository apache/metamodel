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
package org.apache.metamodel.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.apache.metamodel.schema.AbstractTable;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.LazyRef;
import org.apache.metamodel.util.Ref;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

/**
 * Table implementation for Salesforce, which lazy loads columns based on the
 * "describe" web services.
 */
final class SalesforceTable extends AbstractTable {

    private static final long serialVersionUID = 1L;

    private final transient Ref<List<Column>> _columnRef;
    private final transient PartnerConnection _connection;
    private final String _name;
    private final String _remarks;
    private final Schema _schema;

    public SalesforceTable(String name, String remarks, Schema schema, PartnerConnection connection) {
        _name = name;
        _remarks = remarks;
        _schema = schema;
        _connection = connection;
        _columnRef = new LazyRef<List<Column>>() {
            @Override
            protected List<Column> fetch() {
                final List<Column> result = new ArrayList<Column>();
                final DescribeSObjectResult describeSObject;
                try {
                    describeSObject = _connection.describeSObject(_name);
                } catch (ConnectionException e) {
                    throw SalesforceUtils.wrapException(e, "Failed to invoke describeSObject service");
                }
                final Field[] fields = describeSObject.getFields();

                int i = 0;
                for (final Field field : fields) {
                    final String columnName = field.getName();
                    final String columnLabel = field.getLabel();
                    final Boolean nillable = field.isNillable();
                    final FieldType type = field.getType();
                    final Integer columnSize = field.getLength();
                    final ColumnType columnType = toColumnType(type);

                    final MutableColumn column = new MutableColumn(columnName, columnType);
                    column.setTable(SalesforceTable.this);
                    column.setRemarks(columnLabel);
                    column.setNullable(nillable);
                    column.setNativeType(type.toString());
                    column.setColumnSize(columnSize);
                    column.setColumnNumber(i);

                    if (type == FieldType.id) {
                        column.setPrimaryKey(true);
                    }

                    i++;

                    result.add(column);
                }
                return result;
            }
        };
    }

    protected static ColumnType toColumnType(FieldType type) {
        switch (type) {
        case _boolean:
            return ColumnType.BOOLEAN;
        case _int:
            return ColumnType.INTEGER;
        case _double:
        case currency:
            return ColumnType.DOUBLE;
        case date:
            return ColumnType.DATE;
        case datetime:
            return ColumnType.TIMESTAMP;
        case time:
            return ColumnType.TIME;
        case string:
        case email:
        case url:
        case phone:
        case reference:
        case textarea:
        case encryptedstring:
        case base64:
        case id:
        case picklist:
            return ColumnType.VARCHAR;
        default:
            return ColumnType.OTHER;
        }
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Column[] getColumns() {
        if (_columnRef == null) {
            return new Column[0];
        }
        List<Column> columns = _columnRef.get();
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
        return _remarks;
    }

    @Override
    public String getQuote() {
        return null;
    }

}

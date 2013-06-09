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
package org.eobjects.metamodel.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.eobjects.metamodel.schema.AbstractTable;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.schema.MutableColumn;
import org.eobjects.metamodel.schema.Relationship;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.util.LazyRef;
import org.eobjects.metamodel.util.Ref;

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
            return ColumnType.DOUBLE;
        case date:
        case datetime:
        case time:
            return ColumnType.DATE;
        case string:
        case email:
        case url:
        case phone:
        case reference:
        case textarea:
        case encryptedstring:
        case base64:
        case currency:
        case id:
        case picklist:
            return ColumnType.VARCHAR;
        }
        return ColumnType.OTHER;
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

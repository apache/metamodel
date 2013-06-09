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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.util.BooleanComparator;
import org.eobjects.metamodel.util.NumberComparator;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

/**
 * A dataset which reads and pages from web service(s) response of Salesforce.
 */
final class SalesforceDataSet extends AbstractDataSet {

    private final PartnerConnection _connection;
    private final AtomicInteger _recordIndex;
    private QueryResult _queryResult;
    private SObject[] _records;
    private SObject _record;

    public SalesforceDataSet(Column[] columns, QueryResult queryResult, PartnerConnection connection) {
        super(columns);
        _connection = connection;
        _queryResult = queryResult;
        _records = _queryResult.getRecords();
        _recordIndex = new AtomicInteger();
        _record = null;
    }

    @Override
    public boolean next() {
        final int index = _recordIndex.getAndIncrement();
        if (index >= _records.length) {
            if (_queryResult.isDone()) {
                _record = null;
                return false;
            }

            // go to next page
            String queryLocator = _queryResult.getQueryLocator();

            try {
                _queryResult = _connection.queryMore(queryLocator);
                _records = _queryResult.getRecords();
                _recordIndex.set(0);
                return next();
            } catch (ConnectionException e) {
                throw SalesforceUtils.wrapException(e, "Failed to invoke queryMore service");
            }
        }

        _record = _records[index];
        return true;
    }

    @Override
    public Row getRow() {
        if (_record == null) {
            return null;
        }
        final DataSetHeader header = getHeader();
        final Object[] values = new Object[header.size()];

        for (int i = 0; i < values.length; i++) {
            final Column column = header.getSelectItem(i).getColumn();
            final String fieldName = column.getName();
            final Object value = _record.getField(fieldName);
            final Object parsedValue = convert(value, column.getType());
            values[i] = parsedValue;
        }

        return new DefaultRow(header, values);
    }

    private Object convert(Object value, ColumnType columnType) {
        if (value instanceof String && !columnType.isLiteral()) {
            if (columnType.isBoolean()) {
                return BooleanComparator.toBoolean(value);
            }
            if (columnType.isNumber()) {
                return NumberComparator.toNumber(columnType.isNumber());
            }
            if (columnType.isTimeBased()) {
                SimpleDateFormat format = new SimpleDateFormat(SalesforceDataContext.SOQL_DATE_FORMAT_IN);
                try {
                    return format.parse(value.toString());
                } catch (ParseException e) {
                    throw new IllegalStateException("Unable to parse date/time value: " + value);
                }
            }
        }
        return value;
    }
}

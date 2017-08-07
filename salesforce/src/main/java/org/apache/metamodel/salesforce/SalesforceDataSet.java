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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.NumberComparator;

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

    public SalesforceDataSet(List<Column> columns, QueryResult queryResult, PartnerConnection connection) {
        super(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
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
                return NumberComparator.toNumber(value);
            }
            if (columnType.isTimeBased()) {
                final SimpleDateFormat dateFormat;
                if (columnType == ColumnType.DATE) {
                    // note: we don't apply the timezone for DATE fields, since
                    // they don't contain time-of-day information.
                    dateFormat = new SimpleDateFormat(SalesforceDataContext.SOQL_DATE_FORMAT_IN, Locale.ENGLISH);
                } else if (columnType == ColumnType.TIME) {
                    dateFormat = new SimpleDateFormat(SalesforceDataContext.SOQL_TIME_FORMAT_IN, Locale.ENGLISH);
                    dateFormat.setTimeZone(SalesforceDataContext.SOQL_TIMEZONE);
                } else {
                    dateFormat = new SimpleDateFormat(SalesforceDataContext.SOQL_DATE_TIME_FORMAT_IN, Locale.ENGLISH);
                    dateFormat.setTimeZone(SalesforceDataContext.SOQL_TIMEZONE);
                }

                try {
                    return dateFormat.parse(value.toString());
                } catch (ParseException e) {
                    throw new IllegalStateException("Unable to parse date/time value: " + value);
                }
            }
        }
        return value;
    }
}

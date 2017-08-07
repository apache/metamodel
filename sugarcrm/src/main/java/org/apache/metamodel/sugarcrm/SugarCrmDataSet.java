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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.metamodel.util.TimeComparator;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.sugarcrm.ws.soap.GetEntryListResultVersion2;
import com.sugarcrm.ws.soap.LinkNamesToFieldsArray;
import com.sugarcrm.ws.soap.SelectFields;
import com.sugarcrm.ws.soap.SugarsoapPortType;

/**
 * DataSet that reads through web service response(s) of SugarCRM.
 */
final class SugarCrmDataSet extends AbstractDataSet {

    private final SugarsoapPortType _service;
    private final String _session;
    private final AtomicInteger _recordIndex;

    private GetEntryListResultVersion2 _entryList;
    private List<Object> _records;
    private Node _record;

    public SugarCrmDataSet(List<Column> columns, SugarsoapPortType service, String session,
            GetEntryListResultVersion2 entryList) {
        super(columns.stream().map(SelectItem::new).collect(Collectors.toList()));
        _recordIndex = new AtomicInteger();
        _service = service;
        _session = session;
        _entryList = entryList;
        _records = _entryList.getEntryList().getAny();
        _record = null;
    }

    protected GetEntryListResultVersion2 getEntryList() {
        return _entryList;
    }

    @Override
    public boolean next() {
        final int index = _recordIndex.getAndIncrement();
        if (index >= _records.size()) {
            final int nextOffset = _entryList.getNextOffset();
            if (nextOffset == _entryList.getTotalCount()) {
                _record = null;
                return false;
            }

            final DataSetHeader header = getHeader();
            final List<SelectItem> selectItems = header.getSelectItems();
            final List<Column> columns = selectItems.stream().map(si -> si.getColumn()).collect(Collectors.toList());
            final String moduleName = selectItems.get(0).getColumn().getTable().getName();
            final SelectFields selectFields = SugarCrmXmlHelper.createSelectFields(columns);

            _entryList = _service.getEntryList(_session, moduleName, "", "", nextOffset, selectFields,
                    new LinkNamesToFieldsArray(), SugarCrmDataContext.FETCH_SIZE, 0, false);
            _records = _entryList.getEntryList().getAny();
            _recordIndex.set(0);
            return next();
        }

        _record = (Node) _records.get(index);
        return true;
    }

    @Override
    public Row getRow() {
        if (_record == null) {
            return null;
        }
        final DataSetHeader header = getHeader();
        final Object[] values = new Object[header.size()];

        final Element nameValueList = SugarCrmXmlHelper.getChildElement(_record, "name_value_list");
        final List<Element> nameValueItems = SugarCrmXmlHelper.getChildElements(nameValueList);
        final Map<String, String> valueMap = new HashMap<String, String>();
        for (Element nameValueItem : nameValueItems) {
            final String name = SugarCrmXmlHelper.getChildElementText(nameValueItem, "name");
            final String value = SugarCrmXmlHelper.getChildElementText(nameValueItem, "value");
            valueMap.put(name, value);
        }

        for (int i = 0; i < values.length; i++) {
            final Column column = header.getSelectItem(i).getColumn();
            final String fieldName = column.getName();
            final String value = valueMap.get(fieldName);
            final Object parsedValue = convert(value, column.getType());
            values[i] = parsedValue;
        }

        return new DefaultRow(header, values);
    }

    private Object convert(final String value, final ColumnType type) {
        if (value == null) {
            return null;
        }
        if (type == null) {
            return value;
        }

        final Object result;
        if (type.isNumber()) {
            result = NumberComparator.toNumber(value);
        } else if (type.isBoolean()) {
            result = BooleanComparator.toBoolean(value);
        } else if (type.isTimeBased()) {
            result = TimeComparator.toDate(value);
        } else {
            result = value;
        }

        if (result == null) {
            throw new IllegalStateException("Failed to convert value '" + value + "' to type: " + type);
        }

        return result;
    }

}

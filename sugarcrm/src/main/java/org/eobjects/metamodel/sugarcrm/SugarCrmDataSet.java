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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DataSetHeader;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.ColumnType;
import org.eobjects.metamodel.util.BooleanComparator;
import org.eobjects.metamodel.util.NumberComparator;
import org.eobjects.metamodel.util.TimeComparator;
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

    public SugarCrmDataSet(Column[] columns, SugarsoapPortType service, String session,
            GetEntryListResultVersion2 entryList) {
        super(columns);
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
            final SelectItem[] selectItems = header.getSelectItems();
            final Column[] columns = new Column[selectItems.length];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = selectItems[i].getColumn();
            }
            final String moduleName = selectItems[0].getColumn().getTable().getName();
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

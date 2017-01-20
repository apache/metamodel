package org.apache.metamodel.dynamodb;

import java.util.Iterator;
import java.util.Map;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynamoDbDataSet extends AbstractDataSet {

    private final Iterator<Map<String, AttributeValue>> _iterator;
    private Map<String, AttributeValue> _currentItem;

    public DynamoDbDataSet(Column[] columns, ScanResult result) {
        super(columns);
        _iterator = result.getItems().iterator();
    }

    @Override
    public boolean next() {
        final boolean hasNext = _iterator.hasNext();
        if (hasNext) {
            _currentItem = _iterator.next();
            return true;
        }
        _currentItem = null;
        return false;
    }

    @Override
    public Row getRow() {
        if (_currentItem == null) {
            return null;
        }
        final DataSetHeader header = getHeader();
        final Object[] values = new Object[header.size()];
        for (int i = 0; i < values.length; i++) {
            final AttributeValue attributeValue = _currentItem.get(header.getSelectItem(i).getColumn().getName());
            values[i] = toValue(attributeValue);
        }
        final Row row = new DefaultRow(header, values);
        return row;
    }

    private Object toValue(AttributeValue a) {
        if (a == null || a.isNULL()) {
            return null;
        }
        // dynamo is a bit funky this way ... it has a getter for each possible
        // data type.
        return firstNonNull(a.getB(), a.getBOOL(), a.getBS(), a.getL(), a.getM(), a.getN(), a.getNS(), a.getS(), a
                .getSS());
    }

    private Object firstNonNull(Object... objects) {
        for (Object object : objects) {
            if (object != null) {
                return object;
            }
        }
        return null;
    }

}

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
package org.eobjects.metamodel.couchdb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.StreamingViewResult;
import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.eobjects.metamodel.query.SelectItem;

/**
 * DataSet implementation for couch db.
 */
final class CouchDbDataSet extends AbstractDataSet {

    private final Iterator<org.ektorp.ViewResult.Row> _iterator;
    private final StreamingViewResult _streamingViewResult;
    private DefaultRow _row;

    public CouchDbDataSet(SelectItem[] selectItems, StreamingViewResult streamingViewResult) {
        super(selectItems);
        _streamingViewResult = streamingViewResult;

        _iterator = _streamingViewResult.iterator();
    }

    @Override
    public boolean next() {
        if (_iterator == null || !_iterator.hasNext()) {
            return false;
        }

        final org.ektorp.ViewResult.Row row = _iterator.next();
        final JsonNode node = row.getDocAsNode();
        final int size = getHeader().size();
        final Object[] values = new Object[size];
        for (int i = 0; i < size; i++) {
            final String key = getHeader().getSelectItem(i).getColumn().getName();
            final JsonNode valueNode = node.get(key);
            final Object value;
            if (valueNode == null || valueNode.isNull()) {
                value = null;
            } else if (valueNode.isTextual()) {
                value = valueNode.asText();
            } else if (valueNode.isArray()) {
                value = toList(valueNode);
            } else if (valueNode.isObject()) {
                value = toMap(valueNode);
            } else if (valueNode.isBoolean()) {
                value = valueNode.asBoolean();
            } else if (valueNode.isInt()) {
                value = valueNode.asInt();
            } else if (valueNode.isLong()) {
                value = valueNode.asLong();
            } else if (valueNode.isDouble()) {
                value = valueNode.asDouble();
            } else {
                value = valueNode;
            }
            values[i] = value;
        }

        _row = new DefaultRow(getHeader(), values);

        return true;
    }

    private Map<String, Object> toMap(JsonNode valueNode) {
        if (valueNode == null) {
            return null;
        }
        try {
            return new ObjectMapper().reader(Map.class).readValue(valueNode);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Object toList(JsonNode valueNode) {
        if (valueNode == null) {
            return null;
        }
        try {
            return new ObjectMapper().reader(List.class).readValue(valueNode);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Row getRow() {
        return _row;
    }

    @Override
    public void close() {
        super.close();
        _streamingViewResult.close();
    }

}

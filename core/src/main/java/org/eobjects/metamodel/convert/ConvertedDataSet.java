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
package org.eobjects.metamodel.convert;

import org.eobjects.metamodel.data.AbstractDataSet;
import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.data.DefaultRow;
import org.eobjects.metamodel.data.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DataSet} wrapper/decorator which converts values using
 * {@link TypeConverter}s before returning them to the user.
 */
final class ConvertedDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory.getLogger(ConvertedDataSet.class);

    private final DataSet _dataSet;
    private final TypeConverter<?, ?>[] _converters;

    public ConvertedDataSet(DataSet dataSet, TypeConverter<?, ?>[] converters) {
        super(dataSet.getSelectItems());
        _dataSet = dataSet;
        _converters = converters;
    }

    @Override
    public boolean next() {
        return _dataSet.next();
    }

    @Override
    public Row getRow() {
        Row sourceRow = _dataSet.getRow();
        Object[] values = new Object[_converters.length];
        for (int i = 0; i < values.length; i++) {
            Object value = sourceRow.getValue(i);

            @SuppressWarnings("unchecked")
            TypeConverter<Object, ?> converter = (TypeConverter<Object, ?>) _converters[i];

            if (converter != null) {
                Object virtualValue = converter.toVirtualValue(value);
                logger.debug("Converted physical value {} to {}", value, virtualValue);
                value = virtualValue;
            }
            values[i] = value;
        }
        return new DefaultRow(getHeader(), values);
    }

    @Override
    public void close() {
        _dataSet.close();
    }
}

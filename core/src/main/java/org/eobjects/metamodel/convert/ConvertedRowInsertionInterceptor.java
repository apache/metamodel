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

import java.util.HashMap;
import java.util.Map;

import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.intercept.RowInsertionInterceptor;
import org.eobjects.metamodel.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RowInsertionInterceptor} used for intercepting values in
 * {@link RowInsertionBuilder}s that need to be converted, according to a set of
 * {@link TypeConverter}s.
 * 
 * @see TypeConverter
 * @see Converters
 */
public class ConvertedRowInsertionInterceptor implements RowInsertionInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ConvertedRowInsertionInterceptor.class);

    private final Map<Column, TypeConverter<?, ?>> _converters;

    public ConvertedRowInsertionInterceptor() {
        this(new HashMap<Column, TypeConverter<?, ?>>());
    }

    public ConvertedRowInsertionInterceptor(Map<Column, TypeConverter<?, ?>> converters) {
        _converters = converters;
    }

    public void addConverter(Column column, TypeConverter<?, ?> converter) {
        if (converter == null) {
            _converters.remove(column);
        } else {
            _converters.put(column, converter);
        }
    }

    @Override
    public RowInsertionBuilder intercept(RowInsertionBuilder insert) {
        if (_converters.isEmpty()) {
            return insert;
        }

        logger.debug("Insert statement before conversion: {}", insert);

        insert = Converters.convertRow(insert, _converters);

        logger.debug("Insert statement after conversion:  {}", insert);

        return insert;
    }

}

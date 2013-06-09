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

import org.eobjects.metamodel.intercept.RowUpdationInterceptor;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.update.RowUpdationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertedRowUpdationInterceptor implements RowUpdationInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ConvertedRowUpdationInterceptor.class);

    private final Map<Column, TypeConverter<?, ?>> _converters;

    public ConvertedRowUpdationInterceptor() {
        this(new HashMap<Column, TypeConverter<?, ?>>());
    }

    public ConvertedRowUpdationInterceptor(Map<Column, TypeConverter<?, ?>> converters) {
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
    public RowUpdationBuilder intercept(RowUpdationBuilder update) {
        if (_converters.isEmpty()) {
            return update;
        }

        logger.debug("Update statement after conversion:  {}", update);

        update = Converters.convertRow(update, _converters);
        
        logger.debug("Update statement after conversion:  {}", update);

        return update;
    }

}

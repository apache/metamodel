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

import org.eobjects.metamodel.data.DataSet;
import org.eobjects.metamodel.intercept.DataSetInterceptor;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;

/**
 * A {@link DataSetInterceptor} used for intercepting values in {@link DataSet}s
 * that need to be converted, according to a set of {@link TypeConverter}s.
 * 
 * @see TypeConverter
 * @see Converters
 */
public class ConvertedDataSetInterceptor implements DataSetInterceptor, HasReadTypeConverters {

	private Map<Column, TypeConverter<?, ?>> _converters;

	public ConvertedDataSetInterceptor() {
		this(new HashMap<Column, TypeConverter<?, ?>>());
	}

	public ConvertedDataSetInterceptor(
			Map<Column, TypeConverter<?, ?>> converters) {
		_converters = converters;
	}

	@Override
	public void addConverter(Column column, TypeConverter<?, ?> converter) {
		if (converter == null) {
			_converters.remove(column);
		} else {
			_converters.put(column, converter);
		}
	}
	
	protected Map<Column, TypeConverter<?, ?>> getConverters(DataSet dataSet) {
		return _converters;
	}

	@Override
	public final DataSet intercept(DataSet dataSet) {
		Map<Column, TypeConverter<?, ?>> converters = getConverters(dataSet);
		if (converters.isEmpty()) {
			return dataSet;
		}

		boolean hasConverter = false;
		SelectItem[] selectItems = dataSet.getSelectItems();
		TypeConverter<?, ?>[] converterArray = new TypeConverter[selectItems.length];
		for (int i = 0; i < selectItems.length; i++) {
			SelectItem selectItem = selectItems[i];
			Column column = selectItem.getColumn();
			if (column != null && selectItem.getFunction() == null) {
				TypeConverter<?, ?> converter = converters.get(column);
				if (converter != null) {
					hasConverter = true;
					converterArray[i] = converter;
				}
			}
		}

		if (!hasConverter) {
			return dataSet;
		}

		return new ConvertedDataSet(dataSet, converterArray);
	}

}

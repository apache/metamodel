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
package org.apache.metamodel.convert;

import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.intercept.DataSetInterceptor;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;

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
			if (column != null && selectItem.getAggregateFunction() == null) {
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

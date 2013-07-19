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

import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.TimeComparator;

/**
 * A class capable of detecting/narrowing a string column type to something more
 * specific. Either: Boolean, Integer, Double or Date.
 */
final class ColumnTypeDetector {

	private boolean _booleanPossible = true;
	private boolean _integerPossible = true;
	private boolean _doublePossible = true;
	private boolean _datePossible = true;

	public void registerValue(String stringValue) {
		if (stringValue == null || stringValue.length() == 0) {
			return;
		}
		if (_booleanPossible) {
			try {
				BooleanComparator.parseBoolean(stringValue);
			} catch (IllegalArgumentException e) {
				_booleanPossible = false;
			}
		}
		if (_doublePossible) {
			try {
				Double.parseDouble(stringValue);
			} catch (NumberFormatException e) {
				_doublePossible = false;
				_integerPossible = false;
			}
			// If integer is possible, double will always also be possible,
			// but not nescesarily the other way around
			if (_integerPossible) {
				try {
					Integer.parseInt(stringValue);
				} catch (NumberFormatException e) {
					_integerPossible = false;
				}
			}
		}
		if (_datePossible) {
			if (TimeComparator.toDate(stringValue) == null) {
				_datePossible = false;
			}
		}
	}

	public TypeConverter<?, ?> createConverter() {
		if (_booleanPossible) {
			return new StringToBooleanConverter();
		} else if (_integerPossible) {
			return new StringToIntegerConverter();
		} else if (_doublePossible) {
			return new StringToDoubleConverter();
		} else if (_datePossible) {
			return new StringToDateConverter();
		}
		return null;
	}
}

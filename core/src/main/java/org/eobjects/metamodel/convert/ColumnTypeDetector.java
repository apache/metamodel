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

import org.eobjects.metamodel.util.BooleanComparator;
import org.eobjects.metamodel.util.TimeComparator;

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

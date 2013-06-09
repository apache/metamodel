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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.eobjects.metamodel.util.Func;
import org.eobjects.metamodel.util.TimeComparator;

/**
 * A {@link TypeConverter} that converts String values (on the physical layer)
 * to interpreted {@link Date}s.
 * 
 * @author Kasper Sørensen
 * @author Ankit Kumar
 */
public class StringToDateConverter implements TypeConverter<String, Date> {

	private final Func<Date, String> _serializeFunc;
	private final Func<String, Date> _deserializeFunc;

	/**
	 * Constructs a new {@link StringToDateConverter} which will use the
	 * {@link TimeComparator#toDate(Object)} method for parsing dates and the
	 * {@link DateFormat#MEDIUM} date time format for physical representation.
	 */
	public StringToDateConverter() {
		_deserializeFunc = new Func<String, Date>() {
			@Override
			public Date eval(String stringValue) {
				return TimeComparator.toDate(stringValue);
			}
		};
		_serializeFunc = new Func<Date, String>() {
			@Override
			public String eval(Date date) {
				return DateFormat.getDateTimeInstance(DateFormat.MEDIUM,
						DateFormat.MEDIUM).format(date);
			}
		};
	}

	/**
	 * Constructs a new {@link StringToDateConverter} using a given date
	 * pattern.
	 * 
	 * @param datePattern
	 *            a String date pattern, corresponding to the syntax of a
	 *            {@link SimpleDateFormat}.
	 */
	public StringToDateConverter(String datePattern) {
		this(new SimpleDateFormat(datePattern));
	}

	/**
	 * Constructs a new {@link StringToDateConverter} using a given
	 * {@link DateFormat}.
	 * 
	 * @param dateFormat
	 *            the {@link DateFormat} to use for parsing and formatting
	 *            dates.
	 */
	public StringToDateConverter(final DateFormat dateFormat) {
		if (dateFormat == null) {
			throw new IllegalArgumentException("DateFormat cannot be null");
		}
		_deserializeFunc = new Func<String, Date>() {
			@Override
			public Date eval(String string) {
				try {
					return dateFormat.parse(string);
				} catch (ParseException e) {
					throw new IllegalArgumentException(
							"Could not parse date string: " + string);
				}
			}
		};
		_serializeFunc = new Func<Date, String>() {
			@Override
			public String eval(Date date) {
				return dateFormat.format(date);
			}
		};
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toPhysicalValue(Date virtualValue) {
		if (virtualValue == null) {
			return null;
		}
		return _serializeFunc.eval(virtualValue);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Date toVirtualValue(String physicalValue) {
		if (physicalValue == null || physicalValue.length() == 0) {
			return null;
		}
		return _deserializeFunc.eval(physicalValue);
	}

}

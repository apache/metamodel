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
package org.eobjects.metamodel.util;

/**
 * A sequence based on alphabetic representations. Typically a sequence begin
 * with "A", "B", "C" etc. and go from "Z" to "AA", from "AZ" to "BA" etc.
 * 
 * This sequence is practical for generating column names that correspond to
 * column identifiers in spreadsheets and the like.
 * 
 * @author Kasper Sørensen
 */
public class AlphabeticSequence {

	private StringBuilder _stringBuilder;

	/**
	 * Creates an alphabetic sequence that will have "A" as it's first value, if
	 * iterated using next().
	 */
	public AlphabeticSequence() {
		this(Character.toString((char) ('A' - 1)));
	}

	/**
	 * Creates an alphabetic sequence based on a value
	 * 
	 * @param value
	 */
	public AlphabeticSequence(String value) {
		_stringBuilder = new StringBuilder(value.toUpperCase());
	}

	/**
	 * Gets the current value (ie. doesn't iterate).
	 * 
	 * @return a string identifier, eg. "A", "B", "AA" etc.
	 */
	public String current() {
		return _stringBuilder.toString();
	}

	/**
	 * Iterates to the next value and returns it.
	 * 
	 * @return a string identifier, eg. "A", "B", "AA" etc.
	 */
	public String next() {
		boolean updated = false;
		int length = _stringBuilder.length();
		for (int i = length - 1; i >= 0; i--) {
			char c = _stringBuilder.charAt(i);
			if (c != 'Z') {
				c = (char) (c + 1);
				_stringBuilder.setCharAt(i, c);
				updated = true;
				break;
			} else {
				_stringBuilder.setCharAt(i, 'A');
			}
		}

		if (!updated) {
			// need to add another char
			_stringBuilder.append('A');
		}
		return current();
	}
}

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

import java.io.Serializable;
import java.util.StringTokenizer;

import org.eobjects.metamodel.query.FilterItem;

/**
 * Represents a pattern with a wildcard character. These are typically used in
 * FilterItems with the LIKE operator.
 * 
 * @see FilterItem
 */
public final class WildcardPattern implements Serializable {

	private static final long serialVersionUID = 857462137797209624L;
	private String _pattern;
	private char _wildcard;
	private boolean _endsWithDelim;

	public WildcardPattern(String pattern, char wildcard) {
		_pattern = pattern;
		_wildcard = wildcard;
		_endsWithDelim = (_pattern.charAt(pattern.length() - 1) == _wildcard);
	}

	public boolean matches(String value) {
		if (value == null) {
			return false;
		}
		StringTokenizer st = new StringTokenizer(_pattern,
				Character.toString(_wildcard));
		int charIndex = 0;
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			charIndex = value.indexOf(token, charIndex);
			if (charIndex == -1) {
				return false;
			}
			charIndex = charIndex + token.length();
		}
		if (!_endsWithDelim) {
			// Unless the last char of the pattern is a wildcard, we need to
			// have reached the end of the string
			if (charIndex != value.length()) {
				return false;
			}
		}
		return true;
	}
}
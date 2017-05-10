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
package org.apache.metamodel.util;

import java.io.Serializable;
import java.util.StringTokenizer;

import org.apache.metamodel.query.FilterItem;

/**
 * Represents a pattern with a wildcard character. These are typically used in
 * FilterItems with the LIKE operator.
 * 
 * @see FilterItem
 */
public final class WildcardPattern implements Serializable {

	private static final long serialVersionUID = 857462137797209624L;
	private final boolean _startsWithDelim;
	private final boolean _endsWithDelim;
	private String _pattern;
	private char _wildcard;

	public WildcardPattern(String pattern, char wildcard) {
		_pattern = pattern;
		_wildcard = wildcard;
		if(_pattern.isEmpty()){
			_startsWithDelim = _endsWithDelim = false;
		} else {
			_startsWithDelim = _pattern.charAt(0) == _wildcard;
			_endsWithDelim = _pattern.charAt(pattern.length() - 1) == _wildcard;
		}
	}

	public boolean matches(String value) {
		if (value == null) {
			return false;
		}
		StringTokenizer st = new StringTokenizer(_pattern,
				Character.toString(_wildcard));
		int charIndex = 0;
		while (st.hasMoreTokens()) {
			int oldIndex = charIndex;
			String token = st.nextToken();
			charIndex = value.indexOf(token, charIndex);
			if (charIndex == -1 || !_startsWithDelim && oldIndex == 0 && charIndex != 0) {
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
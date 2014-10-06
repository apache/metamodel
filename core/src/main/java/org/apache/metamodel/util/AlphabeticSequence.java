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

/**
 * A sequence based on alphabetic representations. Typically a sequence begin
 * with "A", "B", "C" etc. and go from "Z" to "AA", from "AZ" to "BA" etc.
 * 
 * This sequence is practical for generating column names that correspond to
 * column identifiers in spreadsheets and the like.
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

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

import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comparator that can compare numbers of various kinds (short, integer, float,
 * double etc)
 */
public final class NumberComparator implements Comparator<Object> {

	private static final Logger logger = LoggerFactory
			.getLogger(NumberComparator.class);

	private static final Comparator<Object> _instance = new NumberComparator();

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	private NumberComparator() {
	}

	public static Comparable<Object> getComparable(Object o) {
		final Number n = toNumber(o);
		return new Comparable<Object>() {

			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o) {
				return _instance.compare(n, o);
			}

			@Override
			public String toString() {
				return "NumberComparable[number=" + n + "]";
			}

		};
	}

	public int compare(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return 0;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		Number n1 = toNumber(o1);
		Number n2 = toNumber(o2);
		return Double.valueOf(n1.doubleValue()).compareTo(n2.doubleValue());
	}

	public static Number toNumber(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Number) {
			return (Number) value;
		} else if (value instanceof Boolean) {
			if (Boolean.TRUE.equals(value)) {
				return 1;
			} else {
				return 0;
			}
		} else {
			String stringValue = value.toString();
			try {
				return Integer.parseInt(stringValue);
			} catch (NumberFormatException e1) {
				try {
					return Double.parseDouble(stringValue);
				} catch (NumberFormatException e2) {
					logger.warn(
							"Could not convert '{}' to number, returning null",
							value);
					return null;
				}
			}
		}
	}
}
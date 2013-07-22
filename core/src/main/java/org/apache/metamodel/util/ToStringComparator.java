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

/**
 * Uses the toString method for comparison of objects
 */
public final class ToStringComparator implements Comparator<Object> {

	private static Comparator<Object> _instance = new ToStringComparator();

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	private ToStringComparator() {
	}

	public static Comparable<Object> getComparable(final Object o) {
		final String s = o.toString();
		return new Comparable<Object>() {
			
			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o2) {
				return _instance.compare(s, o2);
			}

			@Override
			public String toString() {
				return "ToStringComparable[string=" + s + "]";
			}
		};
	}

	public int compare(Object o1, Object o2) {
		if (o1 == null && o2 == null) {
			return -1;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		return o1.toString().compareTo(o2.toString());
	}
}
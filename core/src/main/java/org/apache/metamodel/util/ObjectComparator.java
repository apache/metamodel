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
 * General purpose comparator to use for objects of various kinds. Prevents
 * NullPointerExceptions and tries to use comparable interface if available and
 * appropriate on incoming objects.
 */
public final class ObjectComparator implements Comparator<Object> {

	private static final Logger logger = LoggerFactory
			.getLogger(ObjectComparator.class);

	private static final Comparator<Object> _instance = new ObjectComparator();

	public static Comparator<Object> getComparator() {
		return _instance;
	}

	private ObjectComparator() {
	}

	public static Comparable<Object> getComparable(final Object o) {
		return new Comparable<Object>() {

			@Override
			public boolean equals(Object obj) {
				return _instance.equals(obj);
			}

			public int compareTo(Object o2) {
				return _instance.compare(o, o2);
			}

			@Override
			public String toString() {
				return "ObjectComparable[object=" + o + "]";
			}
		};
	}

	@SuppressWarnings("unchecked")
	public int compare(final Object o1, final Object o2) {
		logger.debug("compare({},{})", o1, o2);
		if (o1 == null && o2 == null) {
			return 0;
		}
		if (o1 == null) {
			return -1;
		}
		if (o2 == null) {
			return 1;
		}
		if (o1 instanceof Number && o1 instanceof Number) {
			return NumberComparator.getComparator().compare(o1, o2);
		}
		if (TimeComparator.isTimeBased(o1) && TimeComparator.isTimeBased(o2)) {
			return TimeComparator.getComparator().compare(o1, o2);
		}
		if (BooleanComparator.isBoolean(o1) && BooleanComparator.isBoolean(o2)) {
			return BooleanComparator.getComparator().compare(o1, o2);
		}
		if (o1 instanceof Comparable && o2 instanceof Comparable) {
			@SuppressWarnings("rawtypes")
			Comparable c1 = (Comparable) o1;
			@SuppressWarnings("rawtypes")
			Comparable c2 = (Comparable) o2;
			// We can only count on using the comparable interface if o1 and o2
			// are within of the same class or if one is a subclass of the other
			if (c1.getClass().isAssignableFrom(c2.getClass())) {
				return c1.compareTo(o2);
			}
			if (o2.getClass().isAssignableFrom(c1.getClass())) {
				return -1 * c2.compareTo(o1);
			}
		}
		logger.debug("Using ToStringComparator because no apparent better comparison method could be found");
		return ToStringComparator.getComparator().compare(o1, o2);
	}
}
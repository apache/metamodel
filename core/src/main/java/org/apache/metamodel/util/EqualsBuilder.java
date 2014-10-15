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

import java.lang.reflect.Array;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for implementing equals(...) methods.
 */
public final class EqualsBuilder {

	private static final Logger logger = LoggerFactory
			.getLogger(EqualsBuilder.class);
	private boolean equals = true;

	public EqualsBuilder append(boolean b) {
		logger.debug("append({})", b);
		if (equals) {
			equals = b;
		}
		return this;
	}

	public EqualsBuilder append(Object o1, Object o2) {
		if (equals) {
			equals = equals(o1, o2);
		}
		return this;
	}

	public static boolean equals(final Object obj1, final Object obj2) {
		if (obj1 == obj2) {
			return true;
		}
		
		if (obj1 == null || obj2 == null) {
			return false;
		}
		
		Class<? extends Object> class1 = obj1.getClass();
		Class<? extends Object> class2 = obj2.getClass();
		if (class1.isArray()) {
			if (!class2.isArray()) {
				return false;
			} else {
				Class<?> componentType1 = class1.getComponentType();
				Class<?> componentType2 = class2.getComponentType();
				if (!componentType1.equals(componentType2)) {
					return false;
				}

				int length1 = Array.getLength(obj1);
				int length2 = Array.getLength(obj2);
				if (length1 != length2) {
					return false;
				}
				for (int i = 0; i < length1; i++) {
					Object elem1 = Array.get(obj1, i);
					Object elem2 = Array.get(obj2, i);
					if (!equals(elem1, elem2)) {
						return false;
					}
				}
				return true;
			}
		} else {
			if (class2.isArray()) {
				return false;
			}
		}

		return obj1.equals(obj2);
	}

	public boolean isEquals() {
		return equals;
	}
}

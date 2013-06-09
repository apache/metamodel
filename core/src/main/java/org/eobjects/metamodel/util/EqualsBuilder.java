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

import java.lang.reflect.Array;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for implementing equals(...) methods.
 * 
 * @author Kasper Sørensen
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

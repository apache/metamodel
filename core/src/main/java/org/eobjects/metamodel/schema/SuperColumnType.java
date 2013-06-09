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

package org.eobjects.metamodel.schema;

import java.util.Date;

/**
 * Represents an abstract, generalized type of column
 */
public enum SuperColumnType {

	BOOLEAN_TYPE(Boolean.class),

	LITERAL_TYPE(String.class),

	NUMBER_TYPE(Number.class),

	TIME_TYPE(Date.class),

	BINARY_TYPE(byte[].class),

	OTHER_TYPE(Object.class);

	private Class<?> _javaEquivalentClass;

	private SuperColumnType(Class<?> javaEquivalentClass) {
		_javaEquivalentClass = javaEquivalentClass;
	}

	/**
	 * @return a java class that is appropriate for handling column values of
	 *         this column super type
	 */
	public Class<?> getJavaEquivalentClass() {
		return _javaEquivalentClass;
	}
}
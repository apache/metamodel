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
package org.eobjects.metamodel.intercept;

/**
 * Defines a high-level interface for interceptors in MetaModel.
 * 
 * An intereptor can touch, modify, enhance or do other operations on certain
 * object types as they are passed around for execution in MetaModel. There are
 * 5 types of concrete interceptors:
 * 
 * @see QueryInterceptor
 * @see DataSetInterceptor
 * @see RowInsertionInterceptor
 * @see TableCreationInterceptor
 * @see SchemaInterceptor
 * 
 * @author Kasper Sørensen
 * 
 * @param <E>
 *            the type of object to intercept
 */
public interface Interceptor<E> {

	/**
	 * Interception method invoked by MetaModel when the intercepted object is
	 * being activated.
	 * 
	 * @param input
	 *            the intercepted object
	 * @return the intercepted object, or a modification of this if the object
	 *         is to be replaced by the interceptor. The returned object must
	 *         not be null.
	 */
	public E intercept(E input);
}

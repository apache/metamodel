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

/**
 * Represents an abstract function, which is an executable piece of
 * functionality that has an input and an output. A {@link Func} has a return
 * type, unlike an {@link Action}.
 * 
 * @author Kasper Sørensen
 * 
 * @param <I>
 *            the input type
 * @param <O>
 *            the output type
 */
public interface Func<I, O> {

	/**
	 * Evaluates an element and transforms it using this function.
	 * 
	 * @param arg
	 *            the input given to the function
	 * @return the output result of the function
	 */
	public O eval(I arg);
}

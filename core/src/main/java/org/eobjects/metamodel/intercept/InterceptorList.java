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

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a list of interceptors
 * 
 * @author Kasper Sørensen
 * 
 * @param <E>
 *            the thing to intercept
 * 
 * @see Interceptor
 */
public final class InterceptorList<E> {

	private final List<Interceptor<E>> _interceptors = new ArrayList<Interceptor<E>>();

	public void add(Interceptor<E> interceptor) {
		_interceptors.add(interceptor);
	}

	public void remove(Interceptor<E> interceptor) {
		_interceptors.remove(interceptor);
	}

	/**
	 * Gets the first (if any) interceptor of a specific type.
	 * 
	 * @param interceptorClazz
	 * @return
	 */
	public <I extends Interceptor<E>> I getInterceptorOfType(
			Class<I> interceptorClazz) {
		for (Interceptor<?> interceptor : _interceptors) {
			if (interceptorClazz.isAssignableFrom(interceptor.getClass())) {
				@SuppressWarnings("unchecked")
				I result = (I) interceptor;
				return result;
			}
		}
		return null;
	}

	public boolean isEmpty() {
		return _interceptors.isEmpty();
	}

	protected E interceptAll(E input) {
		for (Interceptor<E> interceptor : _interceptors) {
			input = interceptor.intercept(input);
		}
		return input;
	}
}

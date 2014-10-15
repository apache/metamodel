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
package org.apache.metamodel.intercept;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a list of interceptors
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

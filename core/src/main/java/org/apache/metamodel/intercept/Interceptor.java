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

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
package org.apache.metamodel;

/**
 * Unchecked exception used to signal errors occuring in MetaModel.
 * 
 * All MetaModelExceptions represent errors discovered withing the MetaModel
 * framework. Typically these will occur if you have put together a query that
 * is not meaningful or if there is a structural problem in a schema.
 */
public class MetaModelException extends RuntimeException {

	private static final long serialVersionUID = 5455738384633428319L;

	public MetaModelException(String message, Exception cause) {
		super(message, cause);
	}

	public MetaModelException(String message) {
		super(message);
	}

	public MetaModelException(Exception cause) {
		super(cause);
	}

	public MetaModelException() {
		super();
	}
}
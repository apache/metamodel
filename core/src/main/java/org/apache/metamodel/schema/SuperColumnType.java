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
package org.apache.metamodel.schema;

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
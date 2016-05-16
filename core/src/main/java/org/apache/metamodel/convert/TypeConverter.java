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
package org.apache.metamodel.convert;

/**
 * Defines an interface for converting values from and to their physical
 * materializations and their virtual representations.
 * 
 * @see Converters
 * 
 * @param <P>
 *            the physical type of value
 * @param <V>
 *            the virtual type of value
 */
public interface TypeConverter<P, V> {

	/**
	 * Converts a virtual representation of a value into it's physical value.
	 * 
	 * @param virtualValue
	 *            the virtual representation
	 * @return the physical value
	 */
	public P toPhysicalValue(V virtualValue);

	/**
	 * Converts a physical value into it's virtual representation.
	 * 
	 * @param physicalValue
	 *            the physical value
	 * @return the virtual representation
	 */
	public V toVirtualValue(P physicalValue);
}

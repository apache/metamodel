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

import org.apache.metamodel.util.HasName;

/**
 * Super-interface for named structural types in a DataContext.
 */
public interface NamedStructure extends HasName {

	/**
	 * Gets the name of this structure.
	 * 
	 * @return The name of the structure
	 */
	public String getName();

	/**
	 * Gets an optional quote string that is used to enclose the name of this
	 * structure.
	 * 
	 * @return A quote string used to enclose the name or null if none exists.
	 */
	public String getQuote();

	/**
	 * Gets the name, including optional quotes, of this structure.
	 * 
	 * @return The name of the structure with added quote strings.
	 */
	public String getQuotedName();

	/**
	 * Gets a qualified label for later lookup. Typically this qualified label
	 * is formatted with a simple dot separator. For example, for a column a
	 * typical qualified label would be: "MY_SCHEMA.MY_TABLE.MY_COLUMN".
	 * 
	 * The qualified label can be used as a unique identifier for the structure
	 * but is not necessarily directly transferable to SQL syntax.
	 * 
	 * @return a qualified label
	 */
	public String getQualifiedLabel();
}

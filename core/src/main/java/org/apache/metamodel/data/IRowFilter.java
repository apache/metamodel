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
package org.apache.metamodel.data;

/**
 * A filter that is executed client-side because filter criteria are either more
 * dynamic than the Query-functionality offer or because it cannot be expressed
 * using datastore-neutral queries.
 * 
 * @see FilteredDataSet
 */
public interface IRowFilter {

	/**
	 * Filters a row
	 * 
	 * @param row
	 * @return true if the row is valid according to the filter
	 */
	public boolean accept(Row row);
}
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

/**
 * Represents the various types of tables
 */
public enum TableType {

	TABLE, VIEW, SYSTEM_TABLE, GLOBAL_TEMPORARY, LOCAL_TEMPORARY, ALIAS, SYNONYM, OTHER;

	public static final TableType[] DEFAULT_TABLE_TYPES = new TableType[] {
			TableType.TABLE, TableType.VIEW };

	public boolean isMaterialized() {
		switch (this) {
		case TABLE:
		case SYSTEM_TABLE:
			return true;
		default:
			return false;
		}
	}

	/**
	 * Tries to resolve a TableType based on an incoming string/literal. If no
	 * fitting TableType is found, OTHER will be returned.
	 */
	public static TableType getTableType(String literalType) {
		literalType = literalType.toUpperCase();
		if ("TABLE".equals(literalType)) {
			return TABLE;
		}
		if ("VIEW".equals(literalType)) {
			return VIEW;
		}
		if ("SYSTEM_TABLE".equals(literalType)) {
			return SYSTEM_TABLE;
		}
		if ("GLOBAL_TEMPORARY".equals(literalType)) {
			return GLOBAL_TEMPORARY;
		}
		if ("LOCAL_TEMPORARY".equals(literalType)) {
			return LOCAL_TEMPORARY;
		}
		if ("ALIAS".equals(literalType)) {
			return ALIAS;
		}
		if ("SYNONYM".equals(literalType)) {
			return SYNONYM;
		}
		return OTHER;
	}
}
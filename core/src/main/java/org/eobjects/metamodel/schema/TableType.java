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

package org.eobjects.metamodel.schema;

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
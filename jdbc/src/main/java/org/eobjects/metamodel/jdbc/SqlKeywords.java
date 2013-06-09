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
package org.eobjects.metamodel.jdbc;

import java.util.HashSet;
import java.util.Set;

class SqlKeywords {

	private static final Set<String> KEYWORDS;

	static {
		KEYWORDS = new HashSet<String>();
		KEYWORDS.add("SELECT");
		KEYWORDS.add("DISTINCT");
		KEYWORDS.add("AS");
		KEYWORDS.add("COUNT");
		KEYWORDS.add("SUM");
		KEYWORDS.add("MIN");
		KEYWORDS.add("MAX");
		KEYWORDS.add("FROM");
		KEYWORDS.add("WHERE");
		KEYWORDS.add("LIKE");
		KEYWORDS.add("IN");
		KEYWORDS.add("GROUP");
		KEYWORDS.add("BY");
		KEYWORDS.add("HAVING");
		KEYWORDS.add("ORDER");
	}

	public static boolean isKeyword(String str) {
		str = str.toUpperCase();
		return KEYWORDS.contains(str);
	}
}

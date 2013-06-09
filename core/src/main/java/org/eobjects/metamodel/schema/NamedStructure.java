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

import org.eobjects.metamodel.util.HasName;

/**
 * Super-interface for named structural types in a DataContext.
 * 
 * @author Kasper Sørensen
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

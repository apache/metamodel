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

package org.eobjects.metamodel.data;

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
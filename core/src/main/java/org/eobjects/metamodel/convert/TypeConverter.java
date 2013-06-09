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
package org.eobjects.metamodel.convert;

/**
 * Defines an interface for converting values from and to their physical
 * materializations and their virtual representations.
 * 
 * @see ConvertedDataContext
 * 
 * @author Kasper Sørensen
 * @author Ankit Kumar
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

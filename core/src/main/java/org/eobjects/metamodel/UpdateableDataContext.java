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
package org.eobjects.metamodel;

/**
 * Represents a {@link DataContext} that supports updating write-operations.
 * 
 * @author Kasper Sørensen
 */
public interface UpdateableDataContext extends DataContext {

	/**
	 * Submits an {@link UpdateScript} for execution on the {@link DataContext}.
	 * 
	 * Since implementations of the {@link DataContext} vary quite a lot, there
	 * is no golden rule as to how an update script will be executed. But the
	 * implementors should strive towards handling an {@link UpdateScript} as a
	 * single transactional change to the data store.
	 * 
	 * @param update
	 *            the update script to execute
	 */
	public void executeUpdate(UpdateScript update);

}

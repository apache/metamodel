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

import org.eobjects.metamodel.util.Action;

/**
 * Represents any updating operation or update script that can be executed on a
 * {@link UpdateableDataContext}. Users of MetaModel should implement their own
 * {@link UpdateScript} and submit them to the
 * {@link UpdateableDataContext#executeUpdate(UpdateScript)} method for
 * execution.
 * 
 * @author Kasper Sørensen
 */
public interface UpdateScript extends Action<UpdateCallback> {

	/**
	 * Invoked by MetaModel when the update script should be run. User should
	 * implement this method and invoke update operations on the
	 * {@link UpdateCallback}.
	 */
	@Override
	public void run(UpdateCallback callback);
}

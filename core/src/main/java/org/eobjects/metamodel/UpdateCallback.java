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

import org.eobjects.metamodel.create.TableCreatable;
import org.eobjects.metamodel.delete.RowDeletable;
import org.eobjects.metamodel.drop.TableDroppable;
import org.eobjects.metamodel.insert.RowInsertable;
import org.eobjects.metamodel.update.RowUpdateable;

/**
 * An {@link UpdateCallback} is used by an {@link UpdateScript} to perform
 * updates on a {@link DataContext}. Multiple updates (eg. insertion of several
 * rows or creation of multiple tables) can (and should) be performed with a
 * single {@link UpdateCallback}. This pattern guarantees that connections
 * and/or file handles are handled correctly, surrounding the
 * {@link UpdateScript} that is being executed.
 * 
 * @author Kasper Sørensen
 */
public interface UpdateCallback extends TableCreatable, TableDroppable, RowInsertable, RowUpdateable, RowDeletable {

    /**
     * Gets the DataContext on which the update script is being executed.
     * 
     * @return the DataContext on which the update script is being executed.
     */
    public DataContext getDataContext();
}

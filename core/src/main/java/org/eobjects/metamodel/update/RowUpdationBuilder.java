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
package org.eobjects.metamodel.update;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.data.RowBuilder;
import org.eobjects.metamodel.data.WhereClauseBuilder;
import org.eobjects.metamodel.schema.Table;

/**
 * Builder object for row update on a {@link Table}.
 * 
 * @author Kasper Sørensen
 */
public interface RowUpdationBuilder extends RowBuilder<RowUpdationBuilder>, WhereClauseBuilder<RowUpdationBuilder> {

    /**
     * Gets the table that this update pertains to.
     * 
     * @return the table that this update pertains to.
     */
    @Override
    public Table getTable();

    /**
     * Gets a SQL representation of this update operation. Note that the
     * generated SQL is dialect agnostic, so it is not accurately the same as
     * what will be passed to a potential backing database.
     * 
     * @return a SQL representation of this update operation.
     */
    public String toSql();

    /**
     * Commits the row updation operation. This operation will overwrite rows in
     * the {@link DataContext}.
     * 
     * @throws MetaModelException
     *             if the operation was rejected
     */
    public void execute() throws MetaModelException;
}

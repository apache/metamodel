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
package org.eobjects.metamodel.create;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.schema.Table;

/**
 * Builder object for {@link Table} creation.
 * 
 * @author Kasper Sørensen
 */
public interface TableCreationBuilder {

    /**
     * Builds this table's columns based on another {@link Table} which will be
     * used as a prototype. Using this method simplifies the creation of a table
     * that is similar to an existing table.
     * 
     * @param table
     *            the {@link Table} to use as a prototype
     * @return a builder object for further table creation
     */
    public TableCreationBuilder like(Table table);

    /**
     * Adds a column to the current builder
     * 
     * @param name
     * @return
     */
    public ColumnCreationBuilder withColumn(String name);

    /**
     * Returns this builder instance as a table. This allows for inspecting the
     * table that is being built, before it is actually created.
     * 
     * @return a temporary representation of the table being built.
     */
    public Table toTable();

    /**
     * Gets a SQL representation of this create table operation. Note that the
     * generated SQL is dialect agnostic, so it is not accurately the same as
     * what will be passed to a potential backing database.
     * 
     * @return a SQL representation of this create table operation.
     */
    public String toSql();

    /**
     * Commits the built table and requests that the table structure should be
     * written to the {@link DataContext}.
     * 
     * @return the {@link Table} that was build
     * @throws MetaModelException
     *             if the {@link DataContext} was not able to create the table
     */
    public Table execute() throws MetaModelException;
}

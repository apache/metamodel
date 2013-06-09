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

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

import javax.swing.table.TableModel;

import org.eobjects.metamodel.query.SelectItem;

/**
 * Represents a tabular DataSet where values are bound to columns and rows. A
 * DataSet works similarly to a slightly modularized ResultSet when you traverse
 * it - use the next() method to loop through the rows of the DataSet and use
 * the getRow() method to get the current row.
 * 
 * @author Kasper Sørensen
 */
public interface DataSet extends Closeable, Iterable<Row> {

    /**
     * @return the SelectItems that represent the columns of this DataSet
     */
    public SelectItem[] getSelectItems();

    /**
     * Finds the index of a given SelectItem
     * 
     * @param item
     * @return the index (0-based) of the SelectItem or -1 if the SelectItem
     *         doesn't exist in this DataSet.
     */
    public int indexOf(SelectItem item);

    /**
     * Moves forward to the next row.
     * 
     * @return true if there is a next row or false if not.
     */
    public boolean next();

    /**
     * @return the current row.
     */
    public Row getRow();

    /**
     * Closes the DataSet and any resources it may be holding.
     */
    @Override
    public void close();

    /**
     * Converts the DataSet into a TableModel (will load all values into memory).
     * 
     * @deprecated instantiate a new {@link DataSetTableModel} instead.
     */
    @Deprecated
    public TableModel toTableModel();

    /**
     * Converts the DataSet into a list of object arrays (will load all values
     * into memory)
     */
    public List<Object[]> toObjectArrays();

    /**
     * Converts the DataSet into a list of rows (will load all rows into memory)
     */
    public List<Row> toRows();

    /**
     * Converts the DataSet into an Iterator. Note that unlike many
     * {@link Iterable} objects, {@link DataSet}s are unlikely to allow creation
     * of multiple iterators without risking loss of data in each individual
     * iteration loop.
     */
    @Override
    public Iterator<Row> iterator();

}
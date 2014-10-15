/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.data;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;

import javax.swing.table.TableModel;

import org.apache.metamodel.query.SelectItem;

/**
 * Represents a tabular DataSet where values are bound to columns and rows. A
 * DataSet works similarly to a slightly modularized ResultSet when you traverse
 * it - use the next() method to loop through the rows of the DataSet and use
 * the getRow() method to get the current row.
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
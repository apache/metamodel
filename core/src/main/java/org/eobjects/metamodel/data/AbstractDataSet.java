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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.swing.table.TableModel;

import org.eobjects.metamodel.MetaModelHelper;
import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.util.BaseObject;

/**
 * Abstract DataSet implementation. Provides convenient implementations of
 * trivial method and reusable parts of non-trivial methods of a DataSet.
 * 
 * @author Kasper Sørensen
 */
public abstract class AbstractDataSet extends BaseObject implements DataSet {

    private final DataSetHeader _header;

    /**
     * @deprecated use one of the other constructors, to provide header
     *             information.
     */
    @Deprecated
    public AbstractDataSet() {
        _header = null;
    }

    public AbstractDataSet(SelectItem[] selectItems) {
        this(Arrays.asList(selectItems));
    }

    public AbstractDataSet(List<SelectItem> selectItems) {
        this(new CachingDataSetHeader(selectItems));
    }

    /**
     * Constructor appropriate for dataset implementations that wrap other
     * datasets, such as the {@link MaxRowsDataSet}, {@link FilteredDataSet} and
     * more.
     * 
     * @param dataSet
     */
    public AbstractDataSet(DataSet dataSet) {
        if (dataSet instanceof AbstractDataSet) {
            _header = ((AbstractDataSet) dataSet).getHeader();
        } else {
            _header = new CachingDataSetHeader(Arrays.asList(dataSet.getSelectItems()));
        }
    }

    public AbstractDataSet(DataSetHeader header) {
        _header = header;
    }

    public AbstractDataSet(Column[] columns) {
        this(MetaModelHelper.createSelectItems(columns));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SelectItem[] getSelectItems() {
        return getHeader().getSelectItems();
    }

    protected DataSetHeader getHeader() {
        return _header;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int indexOf(SelectItem item) {
        return getHeader().indexOf(item);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final TableModel toTableModel() {
        TableModel tableModel = new DataSetTableModel(this);
        return tableModel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final List<Object[]> toObjectArrays() {
        try {
            List<Object[]> objects = new ArrayList<Object[]>();
            while (next()) {
                Row row = getRow();
                objects.add(row.getValues());
            }
            return objects;
        } finally {
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "DataSet[selectItems=" + Arrays.toString(getSelectItems()) + "]";
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(getClass());
        identifiers.add(getSelectItems());
    }

    @Override
    public List<Row> toRows() {
        try {
            List<Row> result = new ArrayList<Row>();
            while (next()) {
                result.add(getRow());
            }
            return result;
        } finally {
            close();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Row> iterator() {
        return new DataSetIterator(this);
    }
}
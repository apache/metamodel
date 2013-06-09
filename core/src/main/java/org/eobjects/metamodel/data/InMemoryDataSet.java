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

import java.util.Arrays;
import java.util.List;

import org.eobjects.metamodel.query.SelectItem;

/**
 * DataSet implementation based on in-memory data.
 * 
 * @author Kasper Sørensen
 */
public final class InMemoryDataSet extends AbstractDataSet {

    private final List<Row> _rows;
    private int _rowNumber = -1;

    public InMemoryDataSet(Row... rows) {
        this(Arrays.asList(rows));
    }

    public InMemoryDataSet(List<Row> rows) {
        this(getHeader(rows), rows);
    }

    public InMemoryDataSet(DataSetHeader header, Row... rows) {
        super(header);
        _rows = Arrays.asList(rows);
    }

    public InMemoryDataSet(DataSetHeader header, List<Row> rows) {
        super(header);
        _rows = rows;
    }

    private static DataSetHeader getHeader(List<Row> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Cannot hold an empty list of rows, use " + EmptyDataSet.class
                    + " for this");
        }

        final SelectItem[] selectItems = rows.get(0).getSelectItems();

        if (rows.size() > 3) {
            // not that many records - caching will not have body to scale
            return new SimpleDataSetHeader(selectItems);
        }
        return new CachingDataSetHeader(selectItems);
    }

    @Override
    public boolean next() {
        _rowNumber++;
        if (_rowNumber < _rows.size()) {
            return true;
        }
        return false;
    }

    @Override
    public Row getRow() {
        if (_rowNumber < 0 || _rowNumber >= _rows.size()) {
            return null;
        }
        Row row = _rows.get(_rowNumber);
        assert row.size() == getHeader().size();
        return row;
    }

    public List<Row> getRows() {
        return _rows;
    }

    public int size() {
        return _rows.size();
    }
}
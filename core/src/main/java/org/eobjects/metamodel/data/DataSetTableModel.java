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
import java.util.List;

import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import org.eobjects.metamodel.query.SelectItem;
import org.eobjects.metamodel.util.EqualsBuilder;

/**
 * {@link TableModel} implementation which wraps a {@link DataSet} and presents its data.
 * 
 * @author Kasper Sørensen
 * 
 * @since 3.0
 */
public class DataSetTableModel extends AbstractTableModel {

	private static final long serialVersionUID = 267644807447629777L;
	private boolean _materialized;
	private final List<Row> _materializedRows = new ArrayList<Row>();
	private final DataSet _dataSet;
	private final SelectItem[] _selectItems;

	public DataSetTableModel(DataSet dataSet) {
		_dataSet = dataSet;
		_selectItems = dataSet.getSelectItems();
		_materialized = false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(_selectItems) + _materializedRows.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (obj == this) {
			return true;
		}
		if (getClass() == obj.getClass()) {
			DataSetTableModel that = (DataSetTableModel) obj;
			EqualsBuilder eb = new EqualsBuilder();
			eb.append(_materializedRows, that._materializedRows);
			eb.append(_selectItems, that._selectItems);
			return eb.isEquals();
		}
		return false;
	}

	public int getColumnCount() {
		return _selectItems.length;
	}

	public int getRowCount() {
		materialize();
		return _materializedRows.size();
	}

	private void materialize() {
		if (!_materialized) {
		    try {
		        while (_dataSet.next()) {
		            _materializedRows.add(_dataSet.getRow());
		        }
		    } finally {
		        _dataSet.close();
		    }
			_materialized = true;
		}
	}

	public Object getValueAt(int rowIndex, int columnIndex) {
		materialize();
		return _materializedRows.get(rowIndex).getValue(columnIndex);
	}

	@Override
	public String getColumnName(int column) {
		return _selectItems[column].getSuperQueryAlias(false);
	}

	@Override
	public void setValueAt(Object value, int rowIndex, int columnIndex) {
		throw new UnsupportedOperationException(
				"DataSetTableModels are immutable, so setValueAt() method is unsupported.");
	}
}
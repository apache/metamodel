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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.util.EqualsBuilder;

/**
 * {@link TableModel} implementation which wraps a {@link DataSet} and presents its data.
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
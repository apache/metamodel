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


/**
 * Wraps another DataSet and transparently applies a set of filters to it.
 */
public final class FilteredDataSet extends AbstractDataSet implements WrappingDataSet {

	private final DataSet _dataSet;
	private final IRowFilter[] _filters;
	private Row _row;

	public FilteredDataSet(DataSet dataSet, IRowFilter... filters) {
	    super(dataSet);
		_dataSet = dataSet;
		_filters = filters;
	}

	@Override
	public void close() {
		super.close();
		_dataSet.close();
	}
	
	@Override
	public DataSet getWrappedDataSet() {
	    return _dataSet;
	}

	@Override
	public boolean next() {
		boolean next = false;
		while (_dataSet.next()) {
			Row row = _dataSet.getRow();
			for (IRowFilter filter : _filters) {
				next = filter.accept(row);
				if (!next) {
					break;
				}
			}
			if (next) {
				_row = row;
				break;
			}
		}
		return next;
	}

	@Override
	public Row getRow() {
		return _row;
	}
}
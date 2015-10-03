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

import java.util.List;

import org.apache.metamodel.query.SelectItem;

/**
 * {@link DataSet} wrapper for doing subselection.
 */
public final class SubSelectionDataSet extends AbstractDataSet implements WrappingDataSet {

    private final DataSet _dataSet;

    public SubSelectionDataSet(SelectItem[] selectItemsArray, DataSet dataSet) {
        super(selectItemsArray);
        _dataSet = dataSet;
    }

    public SubSelectionDataSet(List<SelectItem> selectItems, DataSet dataSet) {
        super(selectItems);
        _dataSet = dataSet;
    }

    @Override
    public DataSet getWrappedDataSet() {
        return _dataSet;
    }

    @Override
    public boolean next() {
        return _dataSet.next();
    }

    @Override
    public Row getRow() {
        final DataSetHeader header = getHeader();
        return _dataSet.getRow().getSubSelection(header);
    }

    @Override
    public void close() {
        super.close();
        _dataSet.close();
    }
}

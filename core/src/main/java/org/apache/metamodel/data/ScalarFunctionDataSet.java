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

import java.util.Arrays;
import java.util.List;

import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.util.CollectionUtils;

/**
 * A {@link DataSet} that enhances another {@link DataSet} with
 * {@link ScalarFunction}s.
 */
public class ScalarFunctionDataSet extends AbstractDataSet implements WrappingDataSet {

    private final DataSet _dataSet;
    private final List<SelectItem> _scalarFunctionSelectItemsToEvaluate;

    public ScalarFunctionDataSet(List<SelectItem> scalarFunctionSelectItemsToEvaluate, DataSet dataSet) {
        super(CollectionUtils.concat(false, scalarFunctionSelectItemsToEvaluate,
                Arrays.<SelectItem> asList(dataSet.getSelectItems())));
        _scalarFunctionSelectItemsToEvaluate = scalarFunctionSelectItemsToEvaluate;
        _dataSet = dataSet;
    }

    @Override
    public boolean next() {
        return _dataSet.next();
    }

    @Override
    public Row getRow() {
        final DataSetHeader header = getHeader();
        final Object[] values = new Object[header.size()];

        final Row row = _dataSet.getRow();

        int i = 0;
        for (SelectItem selectItem : _scalarFunctionSelectItemsToEvaluate) {
            final SelectItem selectItemWithoutFunction = selectItem.replaceFunction(null);
            final Object value = selectItem.getScalarFunction().evaluate(row, selectItemWithoutFunction);
            values[i] = value;
            i++;
        }

        // copy the values from the row to ensure that they are available
        // downstream also
        final Object[] rowValues = row.getValues();
        System.arraycopy(rowValues, 0, values, i, rowValues.length);

        return new DefaultRow(header, values);
    }

    @Override
    public DataSet getWrappedDataSet() {
        return _dataSet;
    }

}

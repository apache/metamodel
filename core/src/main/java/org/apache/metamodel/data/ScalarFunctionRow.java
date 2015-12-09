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

import org.apache.metamodel.query.ScalarFunction;
import org.apache.metamodel.query.SelectItem;

/**
 * A {@link Row} implementation that applies {@link ScalarFunction}s when
 * requested. This class closely interacts with the
 * {@link ScalarFunctionDataSet}.
 */
final class ScalarFunctionRow extends AbstractRow {

    private static final long serialVersionUID = 1L;

    private final ScalarFunctionDataSet _scalarFunctionDataSet;
    private final Row _row;

    public ScalarFunctionRow(ScalarFunctionDataSet scalarFunctionDataSet, Row row) {
        _scalarFunctionDataSet = scalarFunctionDataSet;
        _row = row;
    }

    @Override
    public Object getValue(int index) throws IndexOutOfBoundsException {
        final List<SelectItem> scalarFunctionSelectItems = _scalarFunctionDataSet
                .getScalarFunctionSelectItemsToEvaluate();
        final int scalarFunctionCount = scalarFunctionSelectItems.size();
        if (index >= scalarFunctionCount) {
            return _row.getValue(index - scalarFunctionCount);
        }
        final SelectItem selectItem = scalarFunctionSelectItems.get(index);
        final SelectItem selectItemWithoutFunction = selectItem.replaceFunction(null);
        return selectItem.getScalarFunction().evaluate(_row, selectItem.getFunctionParameters(), selectItemWithoutFunction);
    }

    @Override
    public Style getStyle(int index) throws IndexOutOfBoundsException {
        final List<SelectItem> scalarFunctionSelectItems = _scalarFunctionDataSet
                .getScalarFunctionSelectItemsToEvaluate();
        final int scalarFunctionCount = scalarFunctionSelectItems.size();
        if (index >= scalarFunctionCount) {
            _row.getStyle(index - scalarFunctionCount);
        }
        return Style.NO_STYLE;
    }

    @Override
    protected DataSetHeader getHeader() {
        return _scalarFunctionDataSet.getHeader();
    }

}

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
package org.apache.metamodel.query;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.Predicate;

import java.util.List;

/**
 * Represents a function that concatenates values.
 */
public final class ConcatFunction extends DefaultScalarFunction {

    @Override
    public Object evaluate(Row row, Object[] parameters, SelectItem operandItem) {
        SelectItem[] selectItems = row.getSelectItems();
        if (parameters.length == 0) {
            throw new IllegalArgumentException("Expecting some parameters to CONCAT function");
        }
        StringBuilder strBuilder = new StringBuilder();
        for (Object parameter: parameters) {
            String parameterAsString = parameter.toString();
            final int startLiteral = parameterAsString.indexOf('\'');
            if (startLiteral == 0) {
                String literalWithoutTicks = parameterAsString.substring(1, parameterAsString.length() - 1);
                strBuilder.append(literalWithoutTicks);
            } else {
                Column column = getColumnValueFromRow(parameterAsString, selectItems);
                strBuilder.append(row.getValue(column));
            }
        }
        return strBuilder;
    }

    //TODO: Change the returning type to Optional when migrating to Java 8
    private Column getColumnValueFromRow(final String columnName, SelectItem[] selectItems) {

        List<SelectItem> items = CollectionUtils.filter(selectItems, new Predicate<SelectItem>() {
            @Override
            public Boolean eval(SelectItem selectItem) {
                return selectItem.getColumn().getName().equals(columnName);
            }
        });
        if (items.size() > 0) return items.get(0).getColumn();
        else return null;
    }

    @Override
    public ColumnType getExpectedColumnType(ColumnType type) {
        // the column type cannot be inferred so null is returned
        return null;
    }

    @Override
    public String getFunctionName() {
        return "CONCAT";
    }

}

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

import java.util.Map;

import org.apache.metamodel.data.Row;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.CollectionUtils;

/**
 * Represents a function that retrieves a value from within a column of type
 * {@link ColumnType#MAP} or similar.
 */
public final class MapValueFunction extends DefaultScalarFunction {
    
    private static final long serialVersionUID = 1L;

    @Override
    public Object evaluate(Row row, Object[] parameters, SelectItem operandItem) {
        if (parameters.length == 0) {
            throw new IllegalArgumentException("Expecting path parameter to MAP_VALUE function");
        }
        Object value = row.getValue(operandItem);
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            return CollectionUtils.find(map, (String) parameters[0]);
        }
        return null;
    }

    @Override
    public ColumnType getExpectedColumnType(ColumnType type) {
        // the column type cannot be inferred so null is returned
        return null;
    }

    @Override
    public String getFunctionName() {
        return "MAP_VALUE";
    }

}

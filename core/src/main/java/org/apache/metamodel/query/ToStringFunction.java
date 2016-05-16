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
import org.apache.metamodel.schema.ColumnType;

public class ToStringFunction extends DefaultScalarFunction {
    
    private static final long serialVersionUID = 1L;

    @Override
    public ColumnType getExpectedColumnType(ColumnType type) {
        if (type.isLiteral()) {
            return type;
        }
        return ColumnType.STRING;
    }

    @Override
    public String getFunctionName() {
        return "TO_STRING";
    }

    @Override
    public Object evaluate(Row row, Object[] parameters, SelectItem item) {
        final Object value = row.getValue(item);
        if (value == null || value instanceof String) {
            return value;
        }
        return String.valueOf(value);
    }

}

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
import org.apache.metamodel.util.NumberComparator;

public class SubstringFunction implements ScalarFunction {

    private static final long serialVersionUID = 1L;

    public static SubstringFunction createSqlStyle() {
        return new SubstringFunction(true, true);
    }

    public static SubstringFunction createJavaStyle() {
        return new SubstringFunction(false, false);
    }

    private final boolean oneBased;
    private final boolean secondParamIsCharCount;

    /**
     * Creates a {@link SubstringFunction}
     * 
     * @param oneBased true if the character index parameters are 1 based, like most SQL SUBSTRING functions (instead of
     *            0 based, like Java).
     * @param secondParamIsCharCount true if the (optional) second parameter is a "character count", like most SQL
     *            SUBSTRING functions (instead of end-index, like Java)
     * 
     */
    public SubstringFunction(boolean oneBased, boolean secondParamIsCharCount) {
        this.oneBased = oneBased;
        this.secondParamIsCharCount = secondParamIsCharCount;
    }

    @Override
    public ColumnType getExpectedColumnType(ColumnType type) {
        return ColumnType.STRING;
    }

    @Override
    public String getFunctionName() {
        if (!oneBased && !secondParamIsCharCount) {
            return "JAVA_SUBSTRING";
        }
        return "SUBSTRING";
    }

    @Override
    public Object evaluate(Row row, Object[] parameters, SelectItem operandItem) {
        final String str = (String) FunctionType.TO_STRING.evaluate(row, null, operandItem);
        if (str == null) {
            return null;
        }
        final int numParameters = parameters == null ? 0 : parameters.length;
        switch (numParameters) {
        case 0:
            return str;
        case 1:
            int begin = toInt(parameters[0]);
            if (oneBased) {
                begin--;
            }
            if (begin >= str.length()) {
                return "";
            }
            return str.substring(begin);
        default:
            int from = toInt(parameters[0]);
            if (oneBased) {
                from--;
            }
            int to = toInt(parameters[1]);
            if (secondParamIsCharCount) {
                to = from + to;
            } else if (oneBased) {
                to--;
            }

            if (from >= str.length() || from > to) {
                return "";
            }
            if (to >= str.length()) {
                return str.substring(from);
            }
            return str.substring(from, to);
        }
    }

    private int toInt(Object parameter) {
        final Number number = NumberComparator.toNumber(parameter);
        if (number == null) {
            throw new IllegalArgumentException("Not a valid substring parameter: " + parameter);
        }
        return Math.max(0, number.intValue());
    }

}

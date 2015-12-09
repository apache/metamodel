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

/**
 * Factory to create AggregateFunctions through its function name.
 *
 */
public class FunctionTypeFactory {

    public static FunctionType get(String functionName) {
        if (functionName == null || functionName.isEmpty()) {
            return null;
        }

        functionName = functionName.toUpperCase();

        switch (functionName) {
        case "COUNT":
            return FunctionType.COUNT;
        case "AVG":
            return FunctionType.AVG;
        case "SUM":
            return FunctionType.SUM;
        case "MAX":
            return FunctionType.MAX;
        case "MIN":
            return FunctionType.MIN;
        case "RANDOM":
        case "RAND":
            return FunctionType.RANDOM;
        case "FIRST":
            return FunctionType.FIRST;
        case "LAST":
            return FunctionType.LAST;
        case "TO_NUMBER":
        case "NUMBER":
        case "TO_NUM":
        case "NUM":
            return FunctionType.TO_NUMBER;
        case "TO_STRING":
        case "STRING":
        case "TO_STR":
        case "STR":
            return FunctionType.TO_STRING;
        case "TO_BOOLEAN":
        case "BOOLEAN":
        case "TO_BOOL":
        case "BOOL":
            return FunctionType.TO_BOOLEAN;
        case "TO_DATE":
        case "DATE":
            return FunctionType.TO_DATE;
        case "MAP_VALUE":
            return FunctionType.MAP_VALUE;
        default:
            return null;
        }
    }
}

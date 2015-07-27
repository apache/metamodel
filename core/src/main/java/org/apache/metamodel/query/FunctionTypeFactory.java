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

public class FunctionTypeFactory {

    public static AggregateFunction get(String functionName) {
        if (functionName.equals("COUNT")) {
            return new DefaultAggregateFunction<Long>("COUNT");
        } else if (functionName.equals("AVG")) {
            return new DefaultAggregateFunction<Double>("AVG");
        } else if (functionName.equals("SUM")) {
            return new DefaultAggregateFunction<Double>("SUM");
        } else if (functionName.equals("MAX")) {
            return new DefaultAggregateFunction<Object>("MAX");
        } else if (functionName.equals("MIN")) {
            return new DefaultAggregateFunction<Object>("MIN");
        } else {
            return null;
        }
    }
}

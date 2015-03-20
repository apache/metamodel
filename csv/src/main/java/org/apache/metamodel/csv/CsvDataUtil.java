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
package org.apache.metamodel.csv;

import java.lang.Double;
import java.lang.Exception;
import java.lang.Float;
import java.lang.Long;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

final class CsvDataUtil {

    public static Object cast(String columnValue) {
        if(columnValue == null) return null;
        if(columnValue.toUpperCase().equals("TRUE") | columnValue.toUpperCase().equals("FALSE")) {
            return Boolean.parseBoolean(columnValue);
        }
        try {
            return Integer.valueOf(columnValue);
        } catch (Exception ex2) {
            try {
                return Long.valueOf(columnValue);
            } catch (Exception ex3) {
                try {
                    return Float.valueOf(columnValue);
                } catch (Exception ex4) {
                    try {
                        return Double.valueOf(columnValue);
                    } catch (Exception ex5) {
                        try {
                            return Date.valueOf(columnValue);
                        } catch (Exception ex6) {
                            try {
                                return Time.valueOf(columnValue);
                            } catch (Exception ex7) {
                                try {
                                    return Timestamp.valueOf(columnValue);
                                } catch (Exception ex8) {
                                    return columnValue;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
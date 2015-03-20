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
import junit.framework.TestCase;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

public class CsvDataUtilTest extends TestCase {

    public void testTypeCasting() {
        String st = "String";
        Integer intV = Integer.MAX_VALUE;
        Long longV = Long.MAX_VALUE;
        Float floatV = Float.MAX_VALUE;
        Boolean boolV = Boolean.TRUE;
        Date dateV = new Date(Calendar.getInstance().getTimeInMillis());
        Timestamp timestampV = new Timestamp(Calendar.getInstance().getTimeInMillis());
        Time timeV = new Time(Calendar.getInstance().getTimeInMillis());

        Object[] objects = {st, boolV, intV, longV, floatV, dateV, timestampV, timeV};
        String[] values = {st, boolV.toString(), intV.toString(), longV.toString(), floatV.toString(), dateV.toString(), timestampV.toString(), timeV.toString()};

        for(int i = 0; i < objects.length; i++) {
            assertEquals(objects[i].getClass().getName(), CsvDataUtil.cast(values[i]).getClass().getName());
        }
    }
}

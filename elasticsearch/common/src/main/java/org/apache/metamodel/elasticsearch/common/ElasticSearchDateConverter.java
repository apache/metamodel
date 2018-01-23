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
package org.apache.metamodel.elasticsearch.common;

import org.apache.metamodel.util.TimeComparator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Util class to convert date strings from ElasticSearch to
 * proper java Dates.
 */
public final class ElasticSearchDateConverter {

    private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateFormat FALLBACK_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

    public static Date tryToConvert(String dateAsString) {
        if (dateAsString == null) {  
            return null;
        }

        try {
            return DEFAULT_DATE_FORMAT.parse(dateAsString);
        } catch (ParseException e) {
            try {
                return FALLBACK_DATE_FORMAT.parse(dateAsString);
            } catch (ParseException e1) {
                return TimeComparator.toDate(dateAsString);
            }
        }
    }
}

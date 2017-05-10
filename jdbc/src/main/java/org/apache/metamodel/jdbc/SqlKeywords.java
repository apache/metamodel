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
package org.apache.metamodel.jdbc;

import java.util.HashSet;
import java.util.Set;

class SqlKeywords {

    private static final Set<String> KEYWORDS;

    static {
        KEYWORDS = new HashSet<String>();
        KEYWORDS.add("SELECT");
        KEYWORDS.add("DISTINCT");
        KEYWORDS.add("AS");
        KEYWORDS.add("COUNT");
        KEYWORDS.add("SUM");
        KEYWORDS.add("MIN");
        KEYWORDS.add("MAX");
        KEYWORDS.add("FROM");
        KEYWORDS.add("WHERE");
        KEYWORDS.add("LIKE");
        KEYWORDS.add("IN");
        KEYWORDS.add("GROUP");
        KEYWORDS.add("BY");
        KEYWORDS.add("HAVING");
        KEYWORDS.add("ORDER");
        KEYWORDS.add("INDEX");
        KEYWORDS.add("PRIMARY");
        KEYWORDS.add("KEY");
        KEYWORDS.add("CONSTRAINT");
        KEYWORDS.add("UNIQUE");
        KEYWORDS.add("IS");
        KEYWORDS.add("NOT");
        KEYWORDS.add("NULL");
        KEYWORDS.add("CREATE");
        KEYWORDS.add("INSERT");
        KEYWORDS.add("INTO");
        KEYWORDS.add("UPDATE");
        KEYWORDS.add("VALUES");
        KEYWORDS.add("DELETE");
        KEYWORDS.add("AND");
        KEYWORDS.add("OR");
        KEYWORDS.add("BEGIN");
        KEYWORDS.add("END");
        KEYWORDS.add("COLUMN");
        KEYWORDS.add("TABLE");
        KEYWORDS.add("SCHEMA");
        KEYWORDS.add("DATABASE");
        KEYWORDS.add("CAST");
    }

    public static boolean isKeyword(String str) {
        str = str.toUpperCase();
        return KEYWORDS.contains(str);
    }
}

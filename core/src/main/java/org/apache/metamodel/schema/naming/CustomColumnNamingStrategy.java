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
package org.apache.metamodel.schema.naming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link ColumnNamingStrategy} that allows the user to supply his own list of
 * custom column names.
 */
public class CustomColumnNamingStrategy implements ColumnNamingStrategy {

    private static final long serialVersionUID = 1L;

    private final List<String> columnNames;

    public CustomColumnNamingStrategy(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public CustomColumnNamingStrategy(String... columnNames) {
        this(Arrays.asList(columnNames));
    }

    @Override
    public ColumnNamingSession startColumnNamingSession() {
        final Iterator<String> iterator = columnNames.iterator();
        return new ColumnNamingSession() {

            @Override
            public String getNextColumnName(ColumnNamingContext ctx) {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }

            @Override
            public void close() {
            }
        };
    }

}

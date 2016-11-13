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

import java.util.HashSet;
import java.util.Set;

/**
 * A {@link ColumnNamingStrategy} that uses the intrinsic column names, but
 * ensures that all column names are unique. When duplicate names are
 * encountered a number will be appended yielding column names like "name",
 * "name_2", "name_3" etc.
 */
public class UniqueColumnNamingStrategy implements ColumnNamingStrategy {

    private static final long serialVersionUID = 1L;

    @Override
    public ColumnNamingSession startColumnNamingSession() {
        return new ColumnNamingSession() {

            private final Set<String> names = new HashSet<>();

            @Override
            public String getNextColumnName(ColumnNamingContext ctx) {
                final String intrinsicName = ctx.getIntrinsicColumnName();
                boolean unique = names.add(intrinsicName);
                if (unique) {
                    return intrinsicName;
                }

                String newName = null;
                for (int i = 2; !unique; i++) {
                    newName = intrinsicName + '_' + i;
                    unique = names.add(newName);
                }
                return newName;
            }

            @Override
            public void close() {
            }
        };
    }

}

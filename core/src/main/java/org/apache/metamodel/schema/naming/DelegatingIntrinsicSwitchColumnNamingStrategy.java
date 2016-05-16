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

/**
 * A {@link ColumnNamingStrategy} that switches between two other
 * {@link ColumnNamingStrategy} delegates depending on the availability of a
 * intrinsic column name.
 */
public class DelegatingIntrinsicSwitchColumnNamingStrategy implements ColumnNamingStrategy {

    private static final long serialVersionUID = 1L;
    private final ColumnNamingStrategy intrinsicStrategy;
    private final ColumnNamingStrategy nonIntrinsicStrategy;

    public DelegatingIntrinsicSwitchColumnNamingStrategy(ColumnNamingStrategy intrinsicStrategy,
            ColumnNamingStrategy nonIntrinsicStrategy) {
        this.intrinsicStrategy = intrinsicStrategy;
        this.nonIntrinsicStrategy = nonIntrinsicStrategy;
    }

    @Override
    public ColumnNamingSession startColumnNamingSession() {
        final ColumnNamingSession intrinsicSession = intrinsicStrategy.startColumnNamingSession();
        final ColumnNamingSession nonIntrinsicSession = nonIntrinsicStrategy.startColumnNamingSession();
        return new ColumnNamingSession() {

            @Override
            public String getNextColumnName(ColumnNamingContext ctx) {
                final String intrinsicColumnName = ctx.getIntrinsicColumnName();
                if (intrinsicColumnName == null || intrinsicColumnName.isEmpty()) {
                    return nonIntrinsicSession.getNextColumnName(ctx);
                }
                return intrinsicSession.getNextColumnName(ctx);
            }

            @Override
            public void close() {
                intrinsicSession.close();
                nonIntrinsicSession.close();
            }
        };
    }
}

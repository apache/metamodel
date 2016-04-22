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
package org.apache.metamodel.schema.builder;

import org.apache.metamodel.util.AlphabeticSequence;

public class AlphabeticColumnNamingStrategy implements ColumnNamingStrategy {

    @Override
    public String getColumnName(ColumnNamingContext ctx) {
        final int columnIndex = ctx.getColumnIndex();
        final AlphabeticSequence seq = new AlphabeticSequence("A");
        // naive way to get to the right value is to iterate - to be optimized
        for (int i = 0; i < columnIndex; i++) {
            seq.next();
        }
        return seq.current();
    }

}

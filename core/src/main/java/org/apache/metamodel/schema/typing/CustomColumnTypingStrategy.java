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
package org.apache.metamodel.schema.typing;

import org.apache.metamodel.schema.ColumnType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link ColumnTypingStrategy} that allows the user to supply his own list of column types
 */
public class CustomColumnTypingStrategy implements ColumnTypingStrategy {

    private static final long serialVersionUID = 1L;

    private final List<ColumnType> columnTypes;


    /**
     * Creates the strategy based on the provided types.
     *
     * @param columnTypes a list of column types to be applied to a table.
     */
    public CustomColumnTypingStrategy( List<ColumnType> columnTypes ) {
        this.columnTypes = columnTypes;
    }


    /**
     * Creates the strategy based on the provided types.
     *
     * @param columnTypes a list of column types to be applied to a table.
     */
    public CustomColumnTypingStrategy( ColumnType... columnTypes ) {
        this( Arrays.asList( columnTypes ) );
    }


    @Override
    public ColumnTypingSession startColumnTypingSession() {
        final Iterator<ColumnType> iterator = columnTypes.iterator();
        return new ColumnTypingSession() {

            @Override
            public ColumnType getNextColumnType( ColumnTypingContext ctx ) {
                if ( iterator.hasNext() ) {
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

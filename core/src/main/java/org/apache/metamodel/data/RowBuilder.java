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
package org.apache.metamodel.data;

import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;

/**
 * Abstract interface for objects that build rows, either for eg. insert or
 * update purposes.
 * 
 * @see RowInsertionBuilder
 * @see RowUpdationBuilder
 * 
 * @param <RB>
 *            the RowBuilder subtype, used for cascading return values
 */
public interface RowBuilder<RB extends RowBuilder<?>> {

    /**
     * Gets the table that this row builder pertains to.
     * 
     * @return the table that this row builder pertains to.
     */
    public Table getTable();

    /**
     * Sets the value of a column, by column index
     * 
     * @param columnIndex
     * @param value
     * @return
     */
    public RB value(int columnIndex, Object value);

    /**
     * Sets the value of a column, by column index
     * 
     * @param columnIndex
     * @param value
     * @param style
     * @return
     */
    public RB value(int columnIndex, Object value, Style style);

    /**
     * Sets the value of a column
     * 
     * @param column
     * @param value
     * @return
     */
    public RB value(Column column, Object value);

    /**
     * Sets the value of a column
     * 
     * @param column
     * @param value
     * @param style
     * @return
     */
    public RB value(Column column, Object value, Style style);

    /**
     * Sets the value of a column, by column name
     * 
     * @param columnName
     * @param value
     * @return
     */
    public RB value(String columnName, Object value);

    /**
     * Sets the value and the style of this value of a column, by column name
     * 
     * @param columnName
     * @param value
     * @param style
     * @return
     */
    public RB value(String columnName, Object value, Style style);

    /**
     * Gets the built record represented as a {@link Row} object.
     * 
     * @return a {@link Row} object as it will appear if committed and queried.
     */
    public Row toRow();

    /**
     * Determines if a column's value has been explicitly specified or not. This
     * can be used to tell explicit NULL values apart from just unspecified
     * values in a statement.
     * 
     * @param column
     *            the column to check
     * @return true if the column's value has been set, or false if not
     */
    public boolean isSet(Column column);
}

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
package org.apache.metamodel;

import java.util.Optional;

/**
 * Represents a summary of changes made in a given
 * {@link UpdateableDataContext#executeUpdate(UpdateScript)} call.
 * 
 * The amount of information available from an update varies a lot between
 * different implementations of {@link UpdateableDataContext}. This interface
 * represents the most common elements of interest, but not all elements may be
 * available for a given update. For this reason any method call on this
 * interface should be considered not guaranteed to return a value (so expect
 * nulls to represent "not known/available").
 */
public interface UpdateSummary {

    /**
     * Gets the number of inserted rows, or null if this number is unknown.
     * 
     * @return an optional row count.
     */
    public Optional<Integer> getInsertedRows();

    /**
     * Gets the number of updated rows, or null if this number is unknown.
     * 
     * @return an optional row count.
     */
    public Optional<Integer> getUpdatedRows();

    /**
     * Gets the number of deleted rows, or null if this number is unknown.
     * 
     * @return an optional row count.
     */
    public Optional<Integer> getDeletedRows();

    /**
     * Gets a collection of keys that was generated as part of the update -
     * typically because INSERTs where executed on an underlying database which
     * generated record IDs for each insert.
     * 
     * @return an optional collection of generated keys.
     */
    public Optional<Iterable<Object>> getGeneratedKeys();
}

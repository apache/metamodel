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
 * Default implementation of {@link UpdateSummary}.
 */
public class DefaultUpdateSummary implements UpdateSummary {

    private static final UpdateSummary UNKNOWN_UPDATES = new DefaultUpdateSummary(null, null, null, null);

    /**
     * Gets an {@link UpdateSummary} object to return when the extent of the
     * updates are unknown.
     * 
     * @return a {@link UpdateSummary} object without any knowledge of updates
     *         performed.
     */
    public static UpdateSummary unknownUpdates() {
        return UNKNOWN_UPDATES;
    }

    private final Integer _insertedRows;
    private final Integer _updatedRows;
    private final Integer _deletedRows;
    private final Iterable<Object> _generatedKeys;

    public DefaultUpdateSummary(Integer insertedRows, Integer updatedRows, Integer deletedRows,
            Iterable<Object> generatedKeys) {
        _insertedRows = insertedRows;
        _updatedRows = updatedRows;
        _deletedRows = deletedRows;
        _generatedKeys = generatedKeys;
    }

    @Override
    public Optional<Integer> getInsertedRows() {
        return Optional.ofNullable(_insertedRows);
    }

    @Override
    public Optional<Integer> getUpdatedRows() {
        return Optional.ofNullable(_updatedRows);
    }

    @Override
    public Optional<Integer> getDeletedRows() {
        return Optional.ofNullable(_deletedRows);
    }

    @Override
    public Optional<Iterable<Object>> getGeneratedKeys() {
        return Optional.ofNullable(_generatedKeys);
    }

}

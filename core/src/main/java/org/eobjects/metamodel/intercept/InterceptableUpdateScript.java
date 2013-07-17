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
package org.eobjects.metamodel.intercept;

import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.update.RowUpdationBuilder;

final class InterceptableUpdateScript implements UpdateScript {

    private final InterceptableDataContext _interceptableDataContext;
    private final UpdateScript _updateScript;
    private final InterceptorList<TableCreationBuilder> _tableCreationInterceptors;
    private final InterceptorList<TableDropBuilder> _tableDropInterceptors;
    private final InterceptorList<RowInsertionBuilder> _rowInsertionInterceptors;
    private final InterceptorList<RowUpdationBuilder> _rowUpdationInterceptors;
    private final InterceptorList<RowDeletionBuilder> _rowDeletionInterceptors;

    public InterceptableUpdateScript(InterceptableDataContext interceptableDataContext, UpdateScript updateScript,
            InterceptorList<TableCreationBuilder> tableCreationInterceptors,
            InterceptorList<TableDropBuilder> tableDropInterceptors,
            InterceptorList<RowInsertionBuilder> rowInsertionInterceptors,
            InterceptorList<RowUpdationBuilder> rowUpdationInterceptors,
            InterceptorList<RowDeletionBuilder> rowDeletionInterceptors) {
        _interceptableDataContext = interceptableDataContext;
        _updateScript = updateScript;
        _tableCreationInterceptors = tableCreationInterceptors;
        _tableDropInterceptors = tableDropInterceptors;
        _rowInsertionInterceptors = rowInsertionInterceptors;
        _rowUpdationInterceptors = rowUpdationInterceptors;
        _rowDeletionInterceptors = rowDeletionInterceptors;
    }

    @Override
    public void run(UpdateCallback callback) {
        UpdateCallback interceptableUpdateCallback = new InterceptableUpdateCallback(_interceptableDataContext,
                callback, _tableCreationInterceptors, _tableDropInterceptors, _rowInsertionInterceptors,
                _rowUpdationInterceptors, _rowDeletionInterceptors);
        _updateScript.run(interceptableUpdateCallback);
    }

}

/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
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

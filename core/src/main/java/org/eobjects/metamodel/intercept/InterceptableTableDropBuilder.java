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

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.schema.Table;

final class InterceptableTableDropBuilder implements TableDropBuilder {

    private final TableDropBuilder _tableDropBuilder;
    private final InterceptorList<TableDropBuilder> _tableDropInterceptors;

    public InterceptableTableDropBuilder(TableDropBuilder tableDropBuilder,
            InterceptorList<TableDropBuilder> tableDropInterceptors) {
        _tableDropBuilder = tableDropBuilder;
        _tableDropInterceptors = tableDropInterceptors;
    }

    @Override
    public Table getTable() {
        return _tableDropBuilder.getTable();
    }

    @Override
    public String toSql() {
        return _tableDropBuilder.toSql();
    }

    @Override
    public void execute() throws MetaModelException {
        TableDropBuilder tableDropBuilder = _tableDropBuilder;
        tableDropBuilder = _tableDropInterceptors.interceptAll(_tableDropBuilder);
        tableDropBuilder.execute();
    }

}

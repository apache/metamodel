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
package org.eobjects.metamodel.excel;

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.drop.AbstractTableDropBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.schema.MutableSchema;
import org.eobjects.metamodel.schema.Table;

final class ExcelDropTableBuilder extends AbstractTableDropBuilder implements TableDropBuilder {

    private ExcelUpdateCallback _updateCallback;

    public ExcelDropTableBuilder(ExcelUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final Table table = getTable();
        _updateCallback.removeSheet(table.getName());
        final MutableSchema schema = (MutableSchema) table.getSchema();
        schema.removeTable(table);
    }

}

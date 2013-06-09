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
package org.eobjects.metamodel.drop;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.UpdateScript;
import org.eobjects.metamodel.UpdateableDataContext;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

/**
 * Represents a single DROP TABLE operation to be applied to a
 * {@link UpdateableDataContext}. Instead of providing a custom implementation
 * of the {@link UpdateScript} interface, one can use this pre-built drop table
 * implementation. Some {@link DataContext}s may even optimize specifically
 * based on the knowledge that there will only be a single table dropped.
 */
public final class DropTable implements UpdateScript {

    private final String _schemaName;
    private final String _tableName;

    public DropTable(Table table) {
        this(table.getSchema().getName(), table.getName());
    }

    public DropTable(String tableName) {
        this((String) null, tableName);
    }

    public DropTable(Schema schema, String tableName) {
        this(schema.getName(), tableName);
    }

    public DropTable(String schemaName, String tableName) {
        _schemaName = schemaName;
        _tableName = tableName;
    }

    @Override
    public void run(UpdateCallback callback) {
        final TableDropBuilder dropBuilder;
        if (_schemaName == null) {
            dropBuilder = callback.dropTable(_tableName);
        } else {
            dropBuilder = callback.dropTable(_schemaName, _tableName);
        }
        dropBuilder.execute();
    }

}

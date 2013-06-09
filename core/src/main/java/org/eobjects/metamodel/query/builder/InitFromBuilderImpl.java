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
package org.eobjects.metamodel.query.builder;

import java.util.Arrays;
import java.util.List;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.query.Query;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;
import org.eobjects.metamodel.util.BaseObject;

public final class InitFromBuilderImpl extends BaseObject implements InitFromBuilder {

    private DataContext dataContext;
    private Query query;

    public InitFromBuilderImpl(DataContext dataContext) {
        this.dataContext = dataContext;
        this.query = new Query();
    }

    @Override
    public TableFromBuilder from(Table table) {
        if (table == null) {
            throw new IllegalArgumentException("table cannot be null");
        }
        return new TableFromBuilderImpl(table, query, dataContext);
    }

    @Override
    public TableFromBuilder from(String schemaName, String tableName) {
        if (schemaName == null) {
            throw new IllegalArgumentException("schemaName cannot be null");
        }
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }
        Schema schema = dataContext.getSchemaByName(schemaName);
        if (schema == null) {
            schema = dataContext.getDefaultSchema();
        }
        return from(schema, tableName);
    }

    @Override
    public TableFromBuilder from(Schema schema, String tableName) {
        Table table = schema.getTableByName(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Nu such table '" + tableName + "' found in schema: " + schema
                    + ". Available tables are: " + Arrays.toString(schema.getTableNames()));
        }
        return from(table);
    }

    @Override
    public TableFromBuilder from(String tableName) {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName cannot be null");
        }
        Table table = dataContext.getTableByQualifiedLabel(tableName);
        if (table == null) {
            throw new IllegalArgumentException("No such table: " + tableName);
        }
        return from(table);
    }

    @Override
    protected void decorateIdentity(List<Object> identifiers) {
        identifiers.add(query);
    }
}
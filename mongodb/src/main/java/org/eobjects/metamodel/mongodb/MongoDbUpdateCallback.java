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
package org.eobjects.metamodel.mongodb;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;

import org.eobjects.metamodel.AbstractUpdateCallback;
import org.eobjects.metamodel.UpdateCallback;
import org.eobjects.metamodel.create.TableCreationBuilder;
import org.eobjects.metamodel.delete.RowDeletionBuilder;
import org.eobjects.metamodel.drop.TableDropBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Schema;
import org.eobjects.metamodel.schema.Table;

import com.mongodb.DBCollection;

final class MongoDbUpdateCallback extends AbstractUpdateCallback implements UpdateCallback, Closeable {

    private final MongoDbDataContext _dataContext;
    private final Map<String, DBCollection> _collections;
    private final WriteConcernAdvisor _writeConcernAdvisor;

    public MongoDbUpdateCallback(MongoDbDataContext dataContext, WriteConcernAdvisor writeConcernAdvisor) {
        super(dataContext);
        _dataContext = dataContext;
        _writeConcernAdvisor = writeConcernAdvisor;
        _collections = new HashMap<String, DBCollection>();
    }

    @Override
    public MongoDbDataContext getDataContext() {
        return _dataContext;
    }
    
    public WriteConcernAdvisor getWriteConcernAdvisor() {
        return _writeConcernAdvisor;
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new MongoDbTableCreationBuilder(this, schema, name);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        return new MongoDbInsertionBuilder(this, table);
    }

    protected void createCollection(String name) {
        DBCollection collection = _dataContext.getMongoDb().createCollection(name, null);
        _collections.put(name, collection);
    }

    protected void removeCollection(String name) {
        DBCollection collection = getCollection(name);
        _collections.remove(name);
        collection.drop();
    }

    protected DBCollection getCollection(String name) {
        DBCollection collection = _collections.get(name);
        if (collection == null) {
            collection = _dataContext.getMongoDb().getCollection(name);
            _collections.put(name, collection);
        }
        return collection;
    }

    @Override
    public void close() {
        _collections.clear();
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) throws UnsupportedOperationException {
        return new MongoDbDropTableBuilder(this, table);
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) throws IllegalArgumentException, IllegalStateException,
            UnsupportedOperationException {
        return new MongoDbDeleteBuilder(this, table);
    }
}

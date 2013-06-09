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

import org.eobjects.metamodel.MetaModelException;
import org.eobjects.metamodel.delete.AbstractRowDeletionBuilder;
import org.eobjects.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

final class MongoDbDeleteBuilder extends AbstractRowDeletionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbDeleteBuilder.class);

    private final MongoDbUpdateCallback _updateCallback;

    public MongoDbDeleteBuilder(MongoDbUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final DBCollection collection = _updateCallback.getCollection(getTable().getName());

        final MongoDbDataContext dataContext = _updateCallback.getDataContext();
        final BasicDBObject query = dataContext.createMongoDbQuery(getTable(), getWhereItems());
        
        WriteConcern writeConcern = _updateCallback.getWriteConcernAdvisor().adviceDeleteQuery(collection, query);
        
        final WriteResult writeResult = collection.remove(query, writeConcern);
        logger.info("Remove returned result: {}", writeResult);
    }

}

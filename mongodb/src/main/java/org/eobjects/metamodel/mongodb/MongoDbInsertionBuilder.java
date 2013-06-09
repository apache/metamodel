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
import org.eobjects.metamodel.insert.AbstractRowInsertionBuilder;
import org.eobjects.metamodel.insert.RowInsertionBuilder;
import org.eobjects.metamodel.schema.Column;
import org.eobjects.metamodel.schema.Table;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

final class MongoDbInsertionBuilder extends AbstractRowInsertionBuilder<MongoDbUpdateCallback> implements RowInsertionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbInsertionBuilder.class);

    public MongoDbInsertionBuilder(MongoDbUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final Column[] columns = getColumns();
        final Object[] values = getValues();

        final BasicDBObject doc = new BasicDBObject();

        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value != null) {
                doc.put(columns[i].getName(), value);
            }
        }

        final MongoDbUpdateCallback updateCallback = getUpdateCallback();
        final DBCollection collection = updateCallback.getCollection(getTable().getName());

        final WriteConcern writeConcern = updateCallback.getWriteConcernAdvisor().adviceInsert(collection, doc);

        final WriteResult writeResult = collection.insert(doc, writeConcern);
        logger.info("Insert returned result: {}", writeResult);
    }
}

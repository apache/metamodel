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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;

/**
 * A simple {@link WriteConcernAdvisor} that always returns the same write
 * concern.
 */
public class SimpleWriteConcernAdvisor implements WriteConcernAdvisor {

    private final WriteConcern _writeConcern;

    public SimpleWriteConcernAdvisor(WriteConcern writeConcern) {
        if (writeConcern == null) {
            throw new IllegalArgumentException("WriteConcern cannot be null");
        }
        _writeConcern = writeConcern;
    }

    @Override
    public WriteConcern adviceDeleteQuery(DBCollection collection, BasicDBObject query) {
        return _writeConcern;
    }

    @Override
    public WriteConcern adviceInsert(DBCollection collection, BasicDBObject document) {
        return _writeConcern;
    }

}

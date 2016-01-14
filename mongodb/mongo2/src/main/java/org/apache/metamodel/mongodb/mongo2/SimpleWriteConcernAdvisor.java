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
package org.apache.metamodel.mongodb.mongo2;

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

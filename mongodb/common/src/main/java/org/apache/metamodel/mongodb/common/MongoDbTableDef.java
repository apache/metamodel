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
package org.apache.metamodel.mongodb.common;

import java.io.Serializable;

import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * Defines a table layout for MongoDB tables. This class can
 * be used as an instruction set for the MongoDB DataContext implementations to specify
 * which collections, which columns (and their types) should be included in the
 * schema structure of a Mongo DB database.
 * 
 * @deprecated use {@link SimpleTableDef} instead.
 */
@Deprecated
public final class MongoDbTableDef extends SimpleTableDef implements Serializable {

    private static final long serialVersionUID = 1L;

    public MongoDbTableDef(String name, String[] columnNames, ColumnType[] columnTypes) {
        super(name, columnNames, columnTypes);
    }

    public MongoDbTableDef(String name, String[] columnNames) {
        super(name, columnNames);
    }
}
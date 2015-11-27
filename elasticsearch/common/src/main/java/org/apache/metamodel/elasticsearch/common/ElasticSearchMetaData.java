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
package org.apache.metamodel.elasticsearch.common;

import org.apache.metamodel.schema.ColumnType;

/**
 * MetaData representation of an ElasticSearch index type.
 *
 * We will map the elasticsearch fields to columns and their
 * types to {@link ColumnType}s.
 */
public class ElasticSearchMetaData {
    
    private final String[] columnNames;
    private final ColumnType[] columnTypes;

    /**
     * Constructs a {@link ElasticSearchMetaData}.
     *
     */
    public ElasticSearchMetaData(String[] names, ColumnType[] types) {
        this.columnNames = names;
        this.columnTypes = types;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }
}

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
package org.apache.metamodel.elasticsearch.rest;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.drop.AbstractTableDropBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.searchbox.indices.mapping.DeleteMapping;

/**
 * {@link TableDropBuilder} for dropping tables (document types) in an
 * ElasticSearch index.
 */
final class JestElasticSearchDropTableBuilder extends AbstractTableDropBuilder {

    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchDropTableBuilder.class);

    private final JestElasticSearchUpdateCallback _updateCallback;

    public JestElasticSearchDropTableBuilder(JestElasticSearchUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {

        final ElasticSearchRestDataContext dataContext = _updateCallback.getDataContext();
        final Table table = getTable();
        final String documentType = table.getName();
        logger.info("Deleting mapping / document type: {}", documentType);

        final DeleteMapping deleteIndex = new DeleteMapping.Builder(dataContext.getIndexName(), documentType).build();

        _updateCallback.execute(deleteIndex);

        final MutableSchema schema = (MutableSchema) table.getSchema();
        schema.removeTable(table);

    }
}

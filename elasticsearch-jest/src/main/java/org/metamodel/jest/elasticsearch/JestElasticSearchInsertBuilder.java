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
package org.metamodel.jest.elasticsearch;

import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

final class JestElasticSearchInsertBuilder extends AbstractRowInsertionBuilder<JestElasticSearchUpdateCallback> {

    private static final Logger logger = LoggerFactory.getLogger(JestElasticSearchInsertBuilder.class);

    public JestElasticSearchInsertBuilder(JestElasticSearchUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final JestElasticSearchDataContext dataContext = getUpdateCallback().getDataContext();
        final String indexName = dataContext.getIndexName();
        final String documentType = getTable().getName();


        final Map<String, Object> source = new HashMap<>();
        final Column[] columns = getColumns();
        final Object[] values = getValues();
        String id = null;
        for (int i = 0; i < columns.length; i++) {
            if (isSet(columns[i])) {
                final String name = columns[i].getName();
                final Object value = values[i];
                if (JestElasticSearchDataContext.FIELD_ID.equals(name)) {
                    if (value != null) {
                        id = value.toString();
                    }
                } else {
                    source.put(name, value);
                }
            }
        }

        assert !source.isEmpty();

        Index index = new Index.Builder(source).index(indexName).type(documentType).id(id).setParameter(
                Parameters.OP_TYPE, "create").build();

        final DocumentResult result = JestClientExecutor.execute(dataContext.getElasticSearchClient(), index);

        logger.debug("Inserted document: id={}", result.getId());
    }

}

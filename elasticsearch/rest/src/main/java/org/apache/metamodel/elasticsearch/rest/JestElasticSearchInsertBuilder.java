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

import java.util.HashMap;
import java.util.Map;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.insert.AbstractRowInsertionBuilder;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;

import io.searchbox.core.Index;
import io.searchbox.params.Parameters;

final class JestElasticSearchInsertBuilder extends AbstractRowInsertionBuilder<JestElasticSearchUpdateCallback> {

    public JestElasticSearchInsertBuilder(JestElasticSearchUpdateCallback updateCallback, Table table) {
        super(updateCallback, table);
    }

    @Override
    public void execute() throws MetaModelException {
        final JestElasticSearchUpdateCallback updateCallback = getUpdateCallback();
        final ElasticSearchRestDataContext dataContext = updateCallback.getDataContext();
        final String indexName = dataContext.getIndexName();
        final String documentType = getTable().getName();

        final Map<String, Object> source = new HashMap<>();
        final Column[] columns = getColumns();
        final Object[] values = getValues();
        String id = null;
        for (int i = 0; i < columns.length; i++) {
            if (isSet(columns[i])) {
                final String columnName = columns[i].getName();

                final Object value = values[i];
                if (ElasticSearchRestDataContext.FIELD_ID.equals(columnName)) {
                    if (value != null) {
                        id = value.toString();
                    }
                } else {
                    final String fieldName = ElasticSearchUtils.getValidatedFieldName(columnName);
                    source.put(fieldName, value);
                }
            }
        }

        assert !source.isEmpty();

        final Index index = new Index.Builder(source).index(indexName).type(documentType).id(id).setParameter(
                Parameters.OP_TYPE, "create").build();

        getUpdateCallback().execute(index);
    }

}

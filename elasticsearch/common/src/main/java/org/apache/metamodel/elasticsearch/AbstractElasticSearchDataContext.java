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
package org.apache.metamodel.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableColumn;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.common.unit.TimeValue;

public abstract class AbstractElasticSearchDataContext extends QueryPostprocessDataContext implements DataContext,
        UpdateableDataContext {

    public static final TimeValue TIMEOUT_SCROLL = TimeValue.timeValueSeconds(60);

    protected final Object elasticSearchClient;

    protected final String indexName;

    // Table definitions that are set from the beginning, not supposed to be
    // changed.
    protected final List<SimpleTableDef> staticTableDefinitions;

    // Table definitions that are discovered, these can change
    protected final List<SimpleTableDef> dynamicTableDefinitions = new ArrayList<>();

    /**
     * Constructs a {@link ElasticSearchRestDataContext}. This constructor
     * accepts a custom array of {@link SimpleTableDef}s which allows the user
     * to define his own view on the indexes in the engine.
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     * @param tableDefinitions
     *            an array of {@link SimpleTableDef}s, which define the table
     *            and column model of the ElasticSearch index.
     */
    public AbstractElasticSearchDataContext(Object client, String indexName, SimpleTableDef... tableDefinitions) {
        super(false);
        if (client == null) {
            throw new IllegalArgumentException("ElasticSearch Client cannot be null");
        }
        if (indexName == null || indexName.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid ElasticSearch Index name: " + indexName);
        }
        this.elasticSearchClient = client;
        this.indexName = indexName;
        this.staticTableDefinitions = (tableDefinitions == null || tableDefinitions.length == 0 ? Collections
                .<SimpleTableDef> emptyList() : Arrays.asList(tableDefinitions));
        this.dynamicTableDefinitions.addAll(Arrays.asList(detectSchema()));
    }

    /**
     * Performs an analysis of the available indexes in an ElasticSearch cluster
     * {@link JestClient} instance and detects the elasticsearch types structure
     * based on the metadata provided by the ElasticSearch java client.
     *
     * @see {@link #detectTable(JsonObject, String)}
     * @return a mutable schema instance, useful for further fine tuning by the
     *         user.
     */
    protected abstract SimpleTableDef[] detectSchema();

    @Override
    protected Schema getMainSchema() throws MetaModelException {
        final MutableSchema theSchema = new MutableSchema(getMainSchemaName());
        for (final SimpleTableDef tableDef : staticTableDefinitions) {
            addTable(theSchema, tableDef);
        }

        final SimpleTableDef[] tables = detectSchema();
        synchronized (this) {
            dynamicTableDefinitions.clear();
            dynamicTableDefinitions.addAll(Arrays.asList(tables));
            for (final SimpleTableDef tableDef : dynamicTableDefinitions) {
                final List<String> tableNames = theSchema.getTableNames();

                if (!tableNames.contains(tableDef.getName())) {
                    addTable(theSchema, tableDef);
                }
            }
        }

        return theSchema;
    }

    private void addTable(final MutableSchema theSchema, final SimpleTableDef tableDef) {
        final MutableTable table = tableDef.toTable().setSchema(theSchema);
        final Column idColumn = table.getColumnByName(ElasticSearchUtils.FIELD_ID);
        if (idColumn != null && idColumn instanceof MutableColumn) {
            final MutableColumn mutableColumn = (MutableColumn) idColumn;
            mutableColumn.setPrimaryKey(true);
        }
        theSchema.addTable(table);
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return indexName;
    }

    /**
     * Gets the name of the index that this {@link DataContext} is working on.
     */
    public String getIndexName() {
        return indexName;
    }

    protected boolean limitMaxRowsIsSet(int maxRows) {
        return (maxRows != -1);
    }
    
    protected static SimpleTableDef[] sortTables(final List<SimpleTableDef> result) {
        final SimpleTableDef[] tableDefArray = result.toArray(new SimpleTableDef[result.size()]);
        Arrays.sort(tableDefArray, (o1, o2) -> o1.getName().compareTo(o2.getName()));
        return tableDefArray;
    }

}

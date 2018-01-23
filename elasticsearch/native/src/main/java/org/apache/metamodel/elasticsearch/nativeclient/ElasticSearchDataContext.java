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
package org.apache.metamodel.elasticsearch.nativeclient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.UpdateSummary;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.elasticsearch.AbstractElasticSearchDataContext;
import org.apache.metamodel.elasticsearch.common.ElasticSearchMetaData;
import org.apache.metamodel.elasticsearch.common.ElasticSearchMetaDataParser;
import org.apache.metamodel.elasticsearch.common.ElasticSearchUtils;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;

/**
 * DataContext implementation for ElasticSearch analytics engine.
 *
 * ElasticSearch has a data storage structure hierarchy that briefly goes like
 * this:
 * <ul>
 * <li>Index</li>
 * <li>Document type (short: Type) (within an index)</li>
 * <li>Documents (of a particular type)</li>
 * </ul>
 *
 * When instantiating this DataContext, an index name is provided. Within this
 * index, each document type is represented as a table.
 *
 * This implementation supports either automatic discovery of a schema or manual
 * specification of a schema, through the {@link SimpleTableDef} class.
 */
public class ElasticSearchDataContext extends AbstractElasticSearchDataContext {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchDataContext.class);

    private String id;


    /**
     * Constructs a {@link ElasticSearchDataContext}. This constructor accepts a
     * custom array of {@link SimpleTableDef}s which allows the user to define
     * his own view on the indexes in the engine.
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     * @param tableDefinitions
     *            an array of {@link SimpleTableDef}s, which define the table
     *            and column model of the ElasticSearch index.
     */
    public ElasticSearchDataContext(Client client, String indexName, SimpleTableDef... tableDefinitions) {
        super(client, indexName, tableDefinitions);
    }

    /**
     * Constructs a {@link ElasticSearchDataContext} and automatically detects
     * the schema structure/view on all indexes (see
     * {@link #detectTable(ClusterState, String, String)}).
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     */
    public ElasticSearchDataContext(Client client, String indexName) {
        this(client, indexName, new SimpleTableDef[0]);
    }

    @Override
    protected SimpleTableDef[] detectSchema() {
        logger.info("Detecting schema for index '{}'", indexName);

        final ClusterStateRequestBuilder clusterStateRequestBuilder = getElasticSearchClient().admin().cluster()
                .prepareState().setIndices(indexName);
        final ClusterState cs = clusterStateRequestBuilder.execute().actionGet().getState();
        
        final List<SimpleTableDef> result = new ArrayList<>();

        final IndexMetaData imd = cs.getMetaData().index(indexName);
        if (imd == null) {
            // index does not exist
            logger.warn("No metadata returned for index name '{}' - no tables will be detected.");
        } else {
            // id of the index, required for delete operations
            final String indexUUID = imd.getIndexUUID();
            setId(indexUUID);
            final ImmutableOpenMap<String, MappingMetaData> mappings = imd.getMappings();
            final ObjectLookupContainer<String> documentTypes = mappings.keys();
            for (final ObjectCursor<?> documentTypeCursor : documentTypes) {
                final String documentType = documentTypeCursor.value.toString();
                try {
                    final SimpleTableDef table = detectTable(cs, indexName, documentType);
                    result.add(table);
                } catch (Exception e) {
                    logger.error("Unexpected error during detectTable for document type '{}'", documentType, e);
                }
            }
        }
        return sortTables(result);
    }

    /**
     * Performs an analysis of an available index type in an ElasticSearch
     * {@link Client} client and tries to detect the index structure based on
     * the metadata provided by the java client.
     *
     * @param cs
     *            the ElasticSearch cluster
     * @param indexName
     *            the name of the index
     * @param documentType
     *            the name of the index type
     * @return a table definition for ElasticSearch.
     */
    public static SimpleTableDef detectTable(ClusterState cs, String indexName, String documentType) throws Exception {
        logger.debug("Detecting table for document type '{}' in index '{}'", documentType, indexName);
        final IndexMetaData imd = cs.getMetaData().index(indexName);
        if (imd == null) {
            // index does not exist
            throw new IllegalArgumentException("No such index: " + indexName);
        }
        final ImmutableOpenMap<String, MappingMetaData> mappings = imd.getMappings();
        final MappingMetaData mappingMetaData = mappings.get(documentType);
        if (mappingMetaData == null) {
            throw new IllegalArgumentException("No such document type in index '" + indexName + "': " + documentType);
        }
        final Map<String, Object> mp = mappingMetaData.getSourceAsMap();
        final Object metadataProperties = mp.get("properties");
        if (metadataProperties != null && metadataProperties instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> metadataPropertiesMap = (Map<String, ?>) metadataProperties;
            final ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metadataPropertiesMap);
            final SimpleTableDef std = new SimpleTableDef(documentType, metaData.getColumnNames(),
                    metaData.getColumnTypes());
            return std;
        }
        throw new IllegalArgumentException("No mapping properties defined for document type '" + documentType
                + "' in index: " + indexName);
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<SelectItem> selectItems,
            List<FilterItem> whereItems, int firstRow, int maxRows) {
        final QueryBuilder queryBuilder = ElasticSearchUtils.createQueryBuilderForSimpleWhere(whereItems,
                LogicalOperator.AND);
        if (queryBuilder != null) {
            // where clause can be pushed down to an ElasticSearch query
            final SearchRequestBuilder searchRequest = createSearchRequest(table, firstRow, maxRows, queryBuilder);
            final SearchResponse response = searchRequest.execute().actionGet();
            return new ElasticSearchDataSet(getElasticSearchClient(), response, selectItems);
        }
        return super.materializeMainSchemaTable(table, selectItems, whereItems, firstRow, maxRows);
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        final SearchRequestBuilder searchRequest = createSearchRequest(table, 1, maxRows, null);
        final SearchResponse response = searchRequest.execute().actionGet();
        return new ElasticSearchDataSet(getElasticSearchClient(), response, columns.stream().map(SelectItem::new)
                .collect(Collectors.toList()));
    }

    private SearchRequestBuilder createSearchRequest(Table table, int firstRow, int maxRows, QueryBuilder queryBuilder) {
        final String documentType = table.getName();
        final SearchRequestBuilder searchRequest = getElasticSearchClient().prepareSearch(indexName).setTypes(documentType);
        if (firstRow > 1) {
            final int zeroBasedFrom = firstRow - 1;
            searchRequest.setFrom(zeroBasedFrom);
        }
        if (limitMaxRowsIsSet(maxRows)) {
            searchRequest.setSize(maxRows);
        } else {
            searchRequest.setScroll(TIMEOUT_SCROLL);
        }

        if (queryBuilder != null) {
            searchRequest.setQuery(queryBuilder);
        }

        return searchRequest;
    }

    @Override
    protected Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems, Column primaryKeyColumn,
            Object keyValue) {
        if (keyValue == null) {
            return null;
        }

        final String documentType = table.getName();
        final String id = keyValue.toString();

        final GetResponse response = getElasticSearchClient().prepareGet(indexName, documentType, id).execute().actionGet();

        if (!response.isExists()) {
            return null;
        }

        final Map<String, Object> source = response.getSource();
        final String documentId = response.getId();

        final DataSetHeader header = new SimpleDataSetHeader(selectItems);

        return ElasticSearchUtils.createRow(source, documentId, header);
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (!whereItems.isEmpty()) {
            // not supported - will have to be done by counting client-side
            return null;
        }
        final String documentType = table.getName();
        final TermQueryBuilder query = QueryBuilders.termQuery("_type", documentType);
        final SearchResponse searchResponse =
                getElasticSearchClient().prepareSearch(indexName).setSource(new SearchSourceBuilder().size(0).query(query))
                        .execute().actionGet();
        return searchResponse.getHits().getTotalHits();
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        final ElasticSearchUpdateCallback callback = new ElasticSearchUpdateCallback(this);
        update.run(callback);
        callback.onExecuteUpdateFinished();
        return callback.getUpdateSummary();
    }

    @Override
    protected void onSchemaCacheRefreshed() {
        getElasticSearchClient().admin().indices().prepareRefresh(indexName).get();
        
        detectSchema();
    }

    /**
     * Gets the {@link Client} that this {@link DataContext} is wrapping.
     *
     * @return
     */
    public Client getElasticSearchClient() {
        return (Client) elasticSearchClient;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}

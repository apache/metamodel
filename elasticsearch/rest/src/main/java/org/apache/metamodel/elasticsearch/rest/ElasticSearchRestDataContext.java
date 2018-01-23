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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.metamodel.BatchUpdateScript;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
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
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class ElasticSearchRestDataContext extends AbstractElasticSearchDataContext {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestDataContext.class);

    // we scroll when more than 400 rows are expected
    private static final int SCROLL_THRESHOLD = 400;

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
    public ElasticSearchRestDataContext(final ElasticSearchRestClient client, final String indexName, final SimpleTableDef... tableDefinitions) {
        super(client, indexName, tableDefinitions);
    }

    /**
     * Constructs a {@link ElasticSearchRestDataContext} and automatically
     * detects the schema structure/view on all indexes (see
     * {@link #detectTable(JsonObject, String)}).
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     */
    public ElasticSearchRestDataContext(final ElasticSearchRestClient client, String indexName) {
        this(client, indexName, new SimpleTableDef[0]);
    }

    @Override
    protected SimpleTableDef[] detectSchema() {
        logger.info("Detecting schema for index '{}'", indexName);

        final Set<Entry<String, Object>> mappings;
        try {
            mappings = getElasticSearchClient().getMappings(indexName);
        } catch (IOException e) {
            logger.error("Failed to retrieve mappings", e);
            throw new MetaModelException("Failed to execute request for index information needed to detect schema", e);
        }

        final List<SimpleTableDef> result = new ArrayList<>();

		if (mappings.isEmpty()) {
			logger.warn("No metadata returned for index name '{}' - no tables will be detected.");
		} else {
			for (Entry<String, Object> mapping : mappings) {
				final String documentType = mapping.getKey();
				
				@SuppressWarnings("unchecked")
                Map<String, Object> mappingConfiguration = (Map<String, Object>) mapping.getValue();
				@SuppressWarnings("unchecked")
                Map<String, Object> properties = (Map<String, Object>) mappingConfiguration.get("properties");
				
				try {
                    final SimpleTableDef table = detectTable(properties, documentType);
					result.add(table);
				} catch (Exception e) {
					logger.error("Unexpected error during detectTable for document type '{}'", documentType, e);
				}
			}
		}
        return sortTables(result);
    }

    @Override
    protected void onSchemaCacheRefreshed() {
        getElasticSearchClient().refresh(indexName);
        
        detectSchema();
    }

    /**
     * Performs an analysis of an available index type in an ElasticSearch
     * {@link JestClient} client and tries to detect the index structure based
     * on the metadata provided by the java client.
     *
     * @param metadataProperties
     *            the ElasticSearch mapping
     * @param documentType
     *            the name of the index type
     * @return a table definition for ElasticSearch.
     */
    private static SimpleTableDef detectTable(final Map<String, Object> metadataProperties, final String documentType) {
        final ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metadataProperties);
        return new SimpleTableDef(documentType, metaData.getColumnNames(), metaData.getColumnTypes());
    }

    @Override
    protected DataSet materializeMainSchemaTable(final Table table, final List<SelectItem> selectItems,
            final List<FilterItem> whereItems, final int firstRow, final int maxRows) {
        final QueryBuilder queryBuilder = ElasticSearchUtils.createQueryBuilderForSimpleWhere(whereItems,
                LogicalOperator.AND);
        if (queryBuilder != null) {
            // where clause can be pushed down to an ElasticSearch query
            SearchSourceBuilder searchSourceBuilder = createSearchRequest(firstRow, maxRows, queryBuilder);
            SearchResponse result = executeSearch(table, searchSourceBuilder, scrollNeeded(maxRows));

            return new ElasticSearchRestDataSet(getElasticSearchClient(), result, selectItems);
        }
        return super.materializeMainSchemaTable(table, selectItems, whereItems, firstRow, maxRows);
    }

    private boolean scrollNeeded(int maxRows) {
        // if either we don't know about max rows or max rows is set higher than threshold
        return !limitMaxRowsIsSet(maxRows) || maxRows > SCROLL_THRESHOLD;
    }

    private SearchResponse executeSearch(final Table table, final SearchSourceBuilder searchSourceBuilder,
            final boolean scroll) {
        final SearchRequest searchRequest = new SearchRequest(new String[] { getIndexName() }, searchSourceBuilder)
                .types(table.getName());

        if (scroll) {
            searchRequest.scroll(TIMEOUT_SCROLL);
        }

        try {
            return getElasticSearchClient().search(searchRequest);
        } catch (IOException e) {
            logger.warn("Could not execute ElasticSearch query", e);
            throw new MetaModelException("Could not execute ElasticSearch query", e);
        }
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<Column> columns, int maxRows) {
        SearchResponse searchResult = executeSearch(table, createSearchRequest(1, maxRows, null), scrollNeeded(
                maxRows));

        return new ElasticSearchRestDataSet(getElasticSearchClient(), searchResult, columns.stream().map(SelectItem::new).collect(Collectors.toList()));
    }

    private SearchSourceBuilder createSearchRequest(int firstRow, int maxRows, QueryBuilder queryBuilder) {
        final SearchSourceBuilder searchRequest = new SearchSourceBuilder();
        if (firstRow > 1) {
            final int zeroBasedFrom = firstRow - 1;
            searchRequest.from(zeroBasedFrom);
        }
        if (limitMaxRowsIsSet(maxRows)) {
            searchRequest.size(maxRows);
        } else {
            searchRequest.size(SCROLL_THRESHOLD);
        }

        if (queryBuilder != null) {
            searchRequest.query(queryBuilder);
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

        
		final DataSetHeader header = new SimpleDataSetHeader(selectItems);

		try {
			return ElasticSearchUtils.createRow(
			        getElasticSearchClient().get(new GetRequest(getIndexName(), documentType, id)).getSource(), id, header);
		} catch (IOException e) {
			logger.warn("Could not execute ElasticSearch query", e);
			throw new MetaModelException("Could not execute ElasticSearch query", e);
		}
	}

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (!whereItems.isEmpty()) {
            // not supported - will have to be done by counting client-side
            return null;
        }
        final String documentType = table.getName();
        final SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("_type", documentType));
        sourceBuilder.size(0);
        
        try {
        	return getElasticSearchClient().search(new SearchRequest(new String[] {getIndexName()}, sourceBuilder)).getHits().getTotalHits();
        } catch (Exception e) {
            logger.warn("Could not execute ElasticSearch get query", e);
            throw new MetaModelException("Could not execute ElasticSearch get query", e);
        }
    }

    @Override
    public UpdateSummary executeUpdate(UpdateScript update) {
        final boolean isBatch = update instanceof BatchUpdateScript;
        final ElasticSearchRestUpdateCallback callback = new ElasticSearchRestUpdateCallback(this, isBatch);
        update.run(callback);
        callback.onExecuteUpdateFinished();
        return callback.getUpdateSummary();
    }

    /**
     * Gets the {@link JestClient} that this {@link DataContext} is wrapping.
     */
    public ElasticSearchRestClient getElasticSearchClient() {
        return (ElasticSearchRestClient) elasticSearchClient;
    }
}

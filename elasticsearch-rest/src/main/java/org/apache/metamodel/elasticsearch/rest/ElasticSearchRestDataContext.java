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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.params.Parameters;
import org.apache.metamodel.*;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.DataSetHeader;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.data.SimpleDataSetHeader;
import org.apache.metamodel.query.FilterItem;
import org.apache.metamodel.query.LogicalOperator;
import org.apache.metamodel.query.OperatorType;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.*;
import org.apache.metamodel.util.CollectionUtils;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
public class ElasticSearchRestDataContext extends QueryPostprocessDataContext implements DataContext, UpdateableDataContext {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestDataContext.class);

    public static final String FIELD_ID = "_id";

    // 1 minute timeout
    public static final String TIMEOUT_SCROLL = "1m";

    private final JestClient elasticSearchClient;

    private final String indexName;
    // Table definitions that are set from the beginning, not supposed to be changed.
    private final List<SimpleTableDef> staticTableDefinitions;

    // Table definitions that are discovered, these can change
    private final List<SimpleTableDef> dynamicTableDefinitions = new ArrayList<>();

    /**
     * Constructs a {@link ElasticSearchRestDataContext}. This constructor accepts a
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
    public ElasticSearchRestDataContext(JestClient client, String indexName, SimpleTableDef... tableDefinitions) {
        if (client == null) {
            throw new IllegalArgumentException("ElasticSearch Client cannot be null");
        }
        if (indexName == null || indexName.trim().length() == 0) {
            throw new IllegalArgumentException("Invalid ElasticSearch Index name: " + indexName);
        }
        this.elasticSearchClient = client;
        this.indexName = indexName;
        this.staticTableDefinitions = Arrays.asList(tableDefinitions);
        this.dynamicTableDefinitions.addAll(Arrays.asList(detectSchema()));
    }

    /**
     * Constructs a {@link ElasticSearchRestDataContext} and automatically detects
     * the schema structure/view on all indexes (see
     * {@link this.detectSchema(JestClient, String)}).
     *
     * @param client
     *            the ElasticSearch client
     * @param indexName
     *            the name of the ElasticSearch index to represent
     */
    public ElasticSearchRestDataContext(JestClient client, String indexName) {
        this(client, indexName, new SimpleTableDef[0]);
    }

    /**
     * Performs an analysis of the available indexes in an ElasticSearch cluster
     * {@link JestClient} instance and detects the elasticsearch types structure
     * based on the metadata provided by the ElasticSearch java client.
     *
     * @see #detectTable(JsonObject, String)
     * @return a mutable schema instance, useful for further fine tuning by the
     *         user.
     */
    private SimpleTableDef[] detectSchema() {
        logger.info("Detecting schema for index '{}'", indexName);

        final JestResult jestResult;
        try {
            final GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).build();
            jestResult = elasticSearchClient.execute(getMapping);
        } catch (Exception e) {
            logger.error("Failed to retrieve mappings" , e);
            throw new MetaModelException("Failed to execute request for index information needed to detect schema", e);
        }

        if(!jestResult.isSucceeded()){
            logger.error("Failed to retrieve mappings; {}", jestResult.getErrorMessage());
            throw new MetaModelException("Failed to retrieve mappings; " + jestResult.getErrorMessage());
        }

        final List<SimpleTableDef> result = new ArrayList<>();

        final Set<Map.Entry<String, JsonElement>> mappings =
                jestResult.getJsonObject().getAsJsonObject(indexName).getAsJsonObject("mappings").entrySet();
        if(mappings.size() == 0){
            logger.warn("No metadata returned for index name '{}' - no tables will be detected.");
        } else {

            for (Map.Entry<String, JsonElement> entry : mappings) {
                final String documentType = entry.getKey();

                try {
                    final SimpleTableDef table = detectTable(entry.getValue().getAsJsonObject().get("properties").getAsJsonObject(), documentType);
                    result.add(table);
                } catch (Exception e) {
                    logger.error("Unexpected error during detectTable for document type '{}'", documentType, e);
                }
            }
        }
        final SimpleTableDef[] tableDefArray = result.toArray(new SimpleTableDef[result.size()]);
        Arrays.sort(tableDefArray, new Comparator<SimpleTableDef>() {
            @Override
            public int compare(SimpleTableDef o1, SimpleTableDef o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

        return tableDefArray;
    }

    /**
     * Performs an analysis of an available index type in an ElasticSearch
     * {@link JestClient} client and tries to detect the index structure based on
     * the metadata provided by the java client.
     *
     * @param metadataProperties
     *            the ElasticSearch mapping
     * @param documentType
     *            the name of the index type
     * @return a table definition for ElasticSearch.
     */
    private static SimpleTableDef detectTable(JsonObject metadataProperties, String documentType) {
        final JestElasticSearchMetaData metaData = JestElasticSearchMetaDataParser.parse(metadataProperties);
        return new SimpleTableDef(documentType, metaData.getColumnNames(),
                metaData.getColumnTypes());
    }

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
                final List<String> tableNames = Arrays.asList(theSchema.getTableNames());

                if (!tableNames.contains(tableDef.getName())) {
                    addTable(theSchema, tableDef);
                }
            }
        }

        return theSchema;
    }

    private void addTable(final MutableSchema theSchema, final SimpleTableDef tableDef) {
        final MutableTable table = tableDef.toTable().setSchema(theSchema);
        final Column idColumn = table.getColumnByName(FIELD_ID);
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

    @Override
    protected DataSet materializeMainSchemaTable(Table table, List<SelectItem> selectItems,
            List<FilterItem> whereItems, int firstRow, int maxRows) {
        final QueryBuilder queryBuilder = createQueryBuilderForSimpleWhere(whereItems, LogicalOperator.AND);
        if (queryBuilder != null) {
            // where clause can be pushed down to an ElasticSearch query
            SearchSourceBuilder searchSourceBuilder = createSearchRequest(firstRow, maxRows, queryBuilder);
            SearchResult result = executeSearch(table, searchSourceBuilder, false);

            return new JestElasticSearchDataSet(elasticSearchClient, result, selectItems);
        }
        return super.materializeMainSchemaTable(table, selectItems, whereItems, firstRow, maxRows);
    }

    private SearchResult executeSearch(Table table, SearchSourceBuilder searchSourceBuilder, boolean scroll) {
        Search.Builder builder = new Search.Builder(searchSourceBuilder.toString()).addIndex(getIndexName()).addType(table.getName());
        if(scroll){
            builder.setParameter(Parameters.SCROLL, TIMEOUT_SCROLL);
        }

        Search search = builder.build();
        SearchResult result;
        try {
            result = elasticSearchClient.execute(search);
        } catch (Exception e){
            logger.warn("Could not execute ElasticSearch query", e);
            throw new MetaModelException("Could not execute ElasticSearch query", e);
        }
        return result;
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int maxRows) {
        SearchResult searchResult = executeSearch(table, createSearchRequest(1, maxRows, null), limitMaxRowsIsSet(maxRows));

        return new JestElasticSearchDataSet(elasticSearchClient, searchResult, columns);
    }

    private SearchSourceBuilder createSearchRequest(int firstRow, int maxRows, QueryBuilder queryBuilder) {
        final SearchSourceBuilder searchRequest = new SearchSourceBuilder();
        if (firstRow > 1) {
            final int zeroBasedFrom = firstRow - 1;
            searchRequest.from(zeroBasedFrom);
        }
        if (limitMaxRowsIsSet(maxRows)) {
            searchRequest.size(maxRows);
        }

        if (queryBuilder != null) {
            searchRequest.query(queryBuilder);
        }

        return searchRequest;
    }

    /**
     * Creates, if possible, a {@link QueryBuilder} object which can be used to
     * push down one or more {@link FilterItem}s to ElasticSearch's backend.
     *
     * @return a {@link QueryBuilder} if one was produced, or null if the items
     *         could not be pushed down to an ElasticSearch query
     */
    QueryBuilder createQueryBuilderForSimpleWhere(List<FilterItem> whereItems,
            LogicalOperator logicalOperator) {
        if (whereItems.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        List<QueryBuilder> children = new ArrayList<>(whereItems.size());
        for (FilterItem item : whereItems) {
            final QueryBuilder itemQueryBuilder;

            if (item.isCompoundFilter()) {
                final List<FilterItem> childItems = Arrays.asList(item.getChildItems());
                itemQueryBuilder = createQueryBuilderForSimpleWhere(childItems, item.getLogicalOperator());
                if (itemQueryBuilder == null) {
                    // something was not supported, so we have to forfeit here
                    // too.
                    return null;
                }
            } else {
                final Column column = item.getSelectItem().getColumn();
                if (column == null) {
                    // unsupported type of where item - must have a column
                    // reference
                    return null;
                }
                final String fieldName = column.getName();
                final Object operand = item.getOperand();
                final OperatorType operator = item.getOperator();

                if (OperatorType.EQUALS_TO.equals(operator)) {
                    if (operand == null) {
                        itemQueryBuilder = JestElasticSearchUtils.getMissingQuery(fieldName);
                    } else {
                        itemQueryBuilder = QueryBuilders.termQuery(fieldName, operand);
                    }
                } else if (OperatorType.DIFFERENT_FROM.equals(operator)) {
                    if (operand == null) {
                        itemQueryBuilder = JestElasticSearchUtils.getExistsQuery(fieldName);
                    } else {
                        itemQueryBuilder = QueryBuilders.boolQuery().mustNot(
                                QueryBuilders.termQuery(fieldName, operand));
                    }
                } else if (OperatorType.IN.equals(operator)) {
                    final List<?> operands = CollectionUtils.toList(operand);
                    itemQueryBuilder = QueryBuilders.termsQuery(fieldName, operands);
                } else {
                    // not (yet) support operator types
                    return null;
                }
            }

            children.add(itemQueryBuilder);
        }

        // just one where item - just return the child query builder
        if (children.size() == 1) {
            return children.get(0);
        }

        // build a bool query
        final BoolQueryBuilder result = QueryBuilders.boolQuery();
        for (QueryBuilder child : children) {
            switch (logicalOperator) {
            case AND:
                result.must(child);
            case OR:
                result.should(child);
            }
        }

        return result;
    }

    @Override
    protected Row executePrimaryKeyLookupQuery(Table table, List<SelectItem> selectItems, Column primaryKeyColumn,
            Object keyValue) {
        if (keyValue == null) {
            return null;
        }

        final String documentType = table.getName();
        final String id = keyValue.toString();

        final Get get = new Get.Builder(indexName, id).type(documentType).build();
        final JestResult getResult = JestClientExecutor.execute(elasticSearchClient, get);

        final DataSetHeader header = new SimpleDataSetHeader(selectItems);

        return JestElasticSearchUtils.createRow(getResult.getJsonObject().get("_source").getAsJsonObject(), id, header);
    }

    @Override
    protected Number executeCountQuery(Table table, List<FilterItem> whereItems, boolean functionApproximationAllowed) {
        if (!whereItems.isEmpty()) {
            // not supported - will have to be done by counting client-side
            return null;
        }
        final String documentType = table.getName();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("_type", documentType));

        Count count = new Count.Builder().addIndex(indexName).query(sourceBuilder.toString()).build();

        CountResult countResult;
        try {
            countResult = elasticSearchClient.execute(count);
        } catch (Exception e){
            logger.warn("Could not execute ElasticSearch get query", e);
            throw new MetaModelException("Could not execute ElasticSearch get query", e);
        }

        return countResult.getCount();
    }

    private boolean limitMaxRowsIsSet(int maxRows) {
        return (maxRows != -1);
    }

    @Override
    public void executeUpdate(UpdateScript update) {
        final JestElasticSearchUpdateCallback callback = new JestElasticSearchUpdateCallback(this);
        update.run(callback);
        callback.onExecuteUpdateFinished();
    }

    /**
     * Gets the {@link JestClient} that this {@link DataContext} is wrapping.
     */
    public JestClient getElasticSearchClient() {
        return elasticSearchClient;
    }

    /**
     * Gets the name of the index that this {@link DataContext} is working on.
     */
    public String getIndexName() {
        return indexName;
    }
}

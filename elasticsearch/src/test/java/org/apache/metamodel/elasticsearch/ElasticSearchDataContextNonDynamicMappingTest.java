package org.apache.metamodel.elasticsearch;

import junit.framework.TestCase;
import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.elasticsearch.utils.EmbeddedElasticsearchServer;
import org.apache.metamodel.schema.ColumnType;
import org.apache.metamodel.schema.Table;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by mdelaet on 5/14/2015.
 */
public class ElasticSearchDataContextNonDynamicMappingTest {

    private static final String indexName = "twitter";
    private static final String indexType = "tweets";
    private static final String mapping = "{\"date_detection\":\"false\",\"properties\":{\"message\":{\"type\":\"string\",\"index\":\"not_analyzed\",\"doc_values\":\"true\"}}}";

    private static EmbeddedElasticsearchServer embeddedElasticsearchServer;
    private static Client client;
    private static UpdateableDataContext dataContext;

    @BeforeClass
    public static void beforeTests() throws Exception {
        embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
        client = embeddedElasticsearchServer.getClient();
        createIndex();

        // The refresh API allows to explicitly refresh one or more index,
        // making all operations performed since the last refresh available for
        // search
        client.admin().indices().prepareRefresh().execute().actionGet();
        dataContext = new ElasticSearchDataContext(client, indexName);
        System.out.println("Embedded ElasticSearch server created!");
    }

    @AfterClass
    public static void afterTests() {
        embeddedElasticsearchServer.shutdown();
        System.out.println("Embedded ElasticSearch server shut down!");
    }

    @Test
    public void testNonDynamicMapingTableNames() throws Exception{
        assertEquals("[tweets]",
                Arrays.toString(dataContext.getDefaultSchema().getTableNames()));

        Table table = dataContext.getDefaultSchema().getTableByName("tweets");

    }

    private static void createIndex() {
        CreateIndexRequest cir = new CreateIndexRequest(indexName);
        CreateIndexResponse response = client.admin().indices().create(cir).actionGet();

        System.out.println("create index: " + response.isAcknowledged());

        PutMappingRequest pmr = new PutMappingRequest(indexName).type(indexType).source(mapping);

        PutMappingResponse response2 = client.admin().indices().putMapping(pmr).actionGet();
        System.out.println("put mapping: " + response2.isAcknowledged());
    }


}

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

import static java.util.Collections.emptySet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.main.MainRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchRestClient extends RestHighLevelClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    public ElasticSearchRestClient(final RestClient restClient) {
        super(restClient);
    }

    public final boolean refresh(final String indexName, final Header... headers) {
        try {
            return performRequest(new MainRequest(), request -> refresh(indexName),
                    ElasticSearchRestClient::convertResponse, emptySet(), headers);
        } catch (IOException e) {
            logger.info("Failed to refresh index \"{}\"", indexName, e);
        }
        return false;
    }

    private static Request refresh(final String indexName) {
        return new Request(HttpPost.METHOD_NAME, "/" + indexName + "/_refresh", Collections.emptyMap(), null);
    }

    public final boolean delete(final String indexName, final Header... headers) throws IOException {
        return performRequest(new MainRequest(), request -> delete(indexName),
                ElasticSearchRestClient::convertResponse, emptySet(), headers);
    }

    private static Request delete(final String indexName) {
        return new Request(HttpDelete.METHOD_NAME, "/" + indexName, Collections.emptyMap(), null);
    }

    public Set<Entry<String, Object>> getMappings(final String indexName, final Header... headers) throws IOException {
        return performRequestAndParseEntity(new GetIndexRequest(), request -> getMappings(indexName), (
                response) -> parseMappings(response, indexName), emptySet(), headers);
    }

    private static Request getMappings(final String indexName) {
        return new Request(HttpGet.METHOD_NAME, "/" + indexName, Collections.emptyMap(), null);
    }

    public final boolean createMapping(final PutMappingRequest putMappingRequest, final Header... headers)
            throws IOException {
        return performRequest(putMappingRequest, request -> putMapping(putMappingRequest),
                ElasticSearchRestClient::convertResponse, emptySet(), headers);
    }

    private static Request putMapping(final PutMappingRequest putMappingRequest) {
        final String endpoint = "/" + putMappingRequest.indices()[0] + "/_mapping/" + putMappingRequest.type();
        final ByteArrayEntity entity = new ByteArrayEntity(putMappingRequest.source().getBytes(),
                ContentType.APPLICATION_JSON);
        return new Request(HttpPut.METHOD_NAME, endpoint, Collections.emptyMap(), entity);
    }

    // Carbon copy of RestHighLevelClient#convertExistsResponse(Response) method, which is unaccessible from this class.
    private static boolean convertResponse(final Response response) {
        return response.getStatusLine().getStatusCode() == 200;
    }

    @SuppressWarnings("unchecked")
    static Set<Entry<String, Object>> parseMappings(final XContentParser response, final String indexName) throws IOException {
        Map<String, Object> schema = (Map<String, Object>) response.map().get(indexName);
        Map<String, Object> tables = (Map<String, Object>) schema.get("mappings");

        return tables.entrySet();
    }

    ActionResponse execute(final ActionRequest request) throws IOException {
        if (request instanceof BulkRequest) {
            return bulk((BulkRequest) request);
        } else if (request instanceof IndexRequest) {
            return index((IndexRequest) request);
        } else if (request instanceof DeleteRequest) {
            return delete((DeleteRequest) request);
        } else if (request instanceof ClearScrollRequest) {
            return clearScroll((ClearScrollRequest) request);
        } else if (request instanceof SearchScrollRequest) {
            return searchScroll((SearchScrollRequest) request);
        }

        return null;
    }
}

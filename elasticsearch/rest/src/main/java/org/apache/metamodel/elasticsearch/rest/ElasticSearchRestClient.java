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
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Instead of using this class just use the {@link RestHighLevelClient} itself. This class was introduced to
 *             support Elasticsearch 5.6.3, when the {@link RestHighLevelClient} didn't offer all needed functionality,
 *             but now it does, so please use that one instead of this one.
 */
@Deprecated
public class ElasticSearchRestClient extends RestHighLevelClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    public ElasticSearchRestClient(final RestClient restClient) {
        super(RestClient.builder(restClient.getNodes().stream().map(Node::getHost).toArray(HttpHost[]::new)));
    }

    public final boolean refresh(final String indexName, final Header... headers) {
        try {
            indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            logger.info("Failed to refresh index \"{}\"", indexName, e);
        }
        return false;
    }

    public Set<Entry<String, Object>> getMappings(final String indexName, final Header... headers) throws IOException {
        return indices()
                .getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
                .mappings()
                .entrySet()
                .stream()
                .map(ElasticSearchRestClient::convertMapping)
                .collect(Collectors.toSet());
    }

    private static Entry<String, Object> convertMapping(final Entry<String, MappingMetaData> entry) {
        return new Entry<String, Object>() {
            @Override
            public String getKey() {
                return entry.getKey();
            }

            @Override
            public Object getValue() {
                return entry.getValue().getSourceAsMap();
            }

            @Override
            public Object setValue(final Object value) {
                return null;
            }
        };
    }

    public final boolean createMapping(final PutMappingRequest putMappingRequest, final Header... headers)
            throws IOException {
        indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        
        return true;
    }
}

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

import static org.easymock.EasyMock.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.metamodel.data.DataSet;
import org.easymock.EasyMock;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

public class ElasticSearchRestDataSetTest {

    @Test
    public void testClose() throws IOException {
        final RestClient client = createMock(RestClient.class);
        final Response clearScrollResponse = createMock(Response.class);
        final HttpEntity httpEntity = createMock(HttpEntity.class);
        final Header header = createMock(Header.class);

        // Instead of mocking all the classes above, I would rather just mock the RestHighLevelClient, because I only
        // want to verify that the clearScroll method is invoked on it when the ElasticSearchRestDataSet is closed, but
        // because of the manner in which the RestHighLevelClient is implemented it's impossible to mock this in a nice
        // manner.
        final RestHighLevelClient restHighLevelClient = new MockRestHighLevelClient(client);
        final SearchResponse searchResponse = createMock(SearchResponse.class);

        final ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId("id");

        expect(searchResponse.getScrollId()).andReturn("id");
        expect(client.performRequest(anyObject())).andReturn(clearScrollResponse);
        expect(clearScrollResponse.getEntity()).andReturn(httpEntity);
        expect(httpEntity.getContentType()).andReturn(header).anyTimes();
        expect(header.getValue()).andReturn("application/*").anyTimes();
        expect(httpEntity.getContent())
                .andReturn(new ByteArrayInputStream("{\"succeeded\": true,\"num_freed\": 3}".getBytes()));

        EasyMock.replay(client, searchResponse, clearScrollResponse, httpEntity, header);

        final DataSet dataSet = new ElasticSearchRestDataSet(restHighLevelClient, searchResponse, Collections
                .emptyList());

        dataSet.close();

        EasyMock.verify(client, searchResponse, clearScrollResponse, httpEntity, header);
    }

    class MockRestHighLevelClient extends RestHighLevelClient {
        MockRestHighLevelClient(final RestClient client) {
            super(client, RestClient::close, Collections.emptyList());
        }
    }
}

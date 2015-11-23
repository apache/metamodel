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
package org.apache.metamodel.neo4j;

import java.io.IOException;
import java.io.StringBufferInputStream;
import java.nio.charset.StandardCharsets;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpVersion;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import com.google.common.io.BaseEncoding;

@SuppressWarnings("deprecation")
public class Neo4jRequestWrapperTest extends Neo4jTestCase {

    private class MockClosableHttpResponse extends BasicHttpResponse implements CloseableHttpResponse {

        public MockClosableHttpResponse(ProtocolVersion ver, int code, String reason) {
            super(ver, code, reason);
        }

        @Override
        public HttpEntity getEntity() {
            BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
            basicHttpEntity.setContent(new StringBufferInputStream("MockContent for BasicHttpEntity"));
            return basicHttpEntity;
        }

        @Override
        public void close() throws IOException {
            // Do nothing
        }

    }

    @Test
    public void testCreateCypherQueryWithAuthentication() {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        CloseableHttpClient mockHttpClient = new CloseableHttpClient() {

            @Override
            public void close() throws IOException {
                // Do nothing
            }

            @Override
            public HttpParams getParams() {
                // Do nothing
                return null;
            }

            @Override
            public ClientConnectionManager getConnectionManager() {
                // Do nothing
                return null;
            }

            @Override
            protected CloseableHttpResponse doExecute(HttpHost target, HttpRequest request, HttpContext context)
                    throws IOException, ClientProtocolException {
                assertTrue(request instanceof HttpPost);
                HttpPost httpPost = (HttpPost) request;

                Header[] headers = httpPost.getHeaders("Authorization");
                assertNotNull(headers);
                assertEquals(1, headers.length);
                String base64Encoded = headers[0].getValue();
                base64Encoded = base64Encoded.replace("Basic ", "");
                String decoded = new String(BaseEncoding.base64().decode(base64Encoded), StandardCharsets.UTF_8);
                assertEquals("testUsername:testPassword", decoded);

                assertEquals("{\"statements\":[{\"statement\":\"MATCH (n) RETURN n;\"}]}",
                        EntityUtils.toString(httpPost.getEntity()));

                CloseableHttpResponse mockResponse = new MockClosableHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
                return mockResponse;
            }
        };

        Neo4jRequestWrapper wrapper = new Neo4jRequestWrapper(mockHttpClient, new HttpHost(getHostname(), getPort()),
                "testUsername", "testPassword", getServiceRoot());
        wrapper.executeCypherQuery("MATCH (n) RETURN n;");
        // Assertions are in the HttpClient
    }

    @Test
    public void testCreateCypherQueryWithoutAuthentication() {
        if (!isConfigured()) {
            System.err.println(getInvalidConfigurationMessage());
            return;
        }

        CloseableHttpClient mockHttpClient = new CloseableHttpClient() {

            @Override
            public void close() throws IOException {
                // Do nothing
            }

            @Override
            public HttpParams getParams() {
                // Do nothing
                return null;
            }

            @Override
            public ClientConnectionManager getConnectionManager() {
                // Do nothing
                return null;
            }

            @Override
            protected CloseableHttpResponse doExecute(HttpHost target, HttpRequest request, HttpContext context)
                    throws IOException, ClientProtocolException {
                assertTrue(request instanceof HttpPost);
                HttpPost httpPost = (HttpPost) request;

                Header[] headers = httpPost.getHeaders("Authorization");
                assertNotNull(headers);
                assertEquals(0, headers.length);

                assertEquals("{\"statements\":[{\"statement\":\"MATCH (n) RETURN n;\"}]}",
                        EntityUtils.toString(httpPost.getEntity()));

                CloseableHttpResponse mockResponse = new MockClosableHttpResponse(HttpVersion.HTTP_1_1, 200, "OK");
                return mockResponse;
            }
        };

        Neo4jRequestWrapper wrapper = new Neo4jRequestWrapper(mockHttpClient, new HttpHost(getHostname(), getPort()),
                getServiceRoot());
        wrapper.executeCypherQuery("MATCH (n) RETURN n;");
        // Assertions are in the HttpClient
    }

}

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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.BaseEncoding;

/**
 * The class takes care of sending an {@link HttpRequestBase} or a Cypher query
 * to the specified Neo4j instance. Also takes care of the authentication.
 *
 */
public class Neo4jRequestWrapper {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jRequestWrapper.class);

    private final CloseableHttpClient _httpClient;
    private final HttpHost _httpHost;
    private final HttpPost _cypherQueryHttpPost;
    private final String _username;
    private final String _password;

    public Neo4jRequestWrapper(final CloseableHttpClient httpClient, final HttpHost httpHost, final String username,
            final String password, final String serviceRoot) {
        _httpClient = httpClient;
        _httpHost = httpHost;
        _username = username;
        _password = password;
        _cypherQueryHttpPost = new HttpPost(serviceRoot + "/transaction/commit");
    }

    public Neo4jRequestWrapper(final CloseableHttpClient httpClient, final HttpHost httpHost,
            final String serviceRoot) {
        this(httpClient, httpHost, null, null, serviceRoot);
    }

    public String executeRestRequest(final HttpRequestBase httpRequest) {
        return executeRestRequest(httpRequest, _username, _password);
    }

    public String executeRestRequest(final HttpRequestBase httpRequest, final String username, final String password) {
        if ((username != null) && (password != null)) {
            String base64credentials =
                    BaseEncoding.base64().encode((username + ":" + password).getBytes(StandardCharsets.UTF_8));
            httpRequest.addHeader("Authorization", "Basic " + base64credentials);
        }

        try {
            final CloseableHttpResponse response = _httpClient.execute(_httpHost, httpRequest);
            if (response.getEntity() != null) {
                return EntityUtils.toString(response.getEntity());
            }
            return null;
        } catch (final IOException e) {
            logger.error("An error occurred while executing " + httpRequest, e);
            throw new IllegalStateException(e);
        }
    }

    public String executeCypherQuery(final String cypherQuery) {
        final JSONObject cypherQueryRequest = new JSONObject();
        final HashMap<String, String> statement = new HashMap<>();
        statement.put("statement", cypherQuery);

        final JSONArray statementsArray = new JSONArray();
        statementsArray.put(statement);

        return executeRequest(cypherQueryRequest, statementsArray);
    }

    public String executeCypherQueries(final List<String> cypherQueries) {
        final JSONObject cypherQueryRequest = new JSONObject();
        final JSONArray statementsArray = new JSONArray();
        for (final String cypherQuery : cypherQueries) {
            final HashMap<String, String> statement = new HashMap<>();
            statement.put("statement", cypherQuery);

            statementsArray.put(statement);
        }

        return executeRequest(cypherQueryRequest, statementsArray);
    }

    private String executeRequest(final JSONObject cypherQueryRequest, final JSONArray statementsArray) {
        try {
            cypherQueryRequest.put("statements", statementsArray);

            final String requestBody = cypherQueryRequest.toString();
            _cypherQueryHttpPost.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

            final String responseJSONString = executeRestRequest(_cypherQueryHttpPost);
            return responseJSONString;
        } catch (final JSONException e) {
            logger.error("Error occurred while constructing JSON request body for " + _cypherQueryHttpPost, e);
            throw new IllegalStateException(e);
        }
    }
}

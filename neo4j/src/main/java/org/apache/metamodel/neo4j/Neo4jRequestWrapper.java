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
import org.apache.http.client.ClientProtocolException;
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

    public Neo4jRequestWrapper(CloseableHttpClient httpClient, HttpHost httpHost, String username, String password,
            String serviceRoot) {
        _httpClient = httpClient;
        _httpHost = httpHost;
        _username = username;
        _password = password;
        _cypherQueryHttpPost = new HttpPost(serviceRoot + "/transaction/commit");
    }

    public Neo4jRequestWrapper(CloseableHttpClient httpClient, HttpHost httpHost, String serviceRoot) {
        this(httpClient, httpHost, null, null, serviceRoot);
    }

    public String executeRestRequest(HttpRequestBase httpRequest) {
        return executeRestRequest(httpRequest, _username, _password);
    }

    public String executeRestRequest(HttpRequestBase httpRequest, String username, String password) {
        if ((username != null) && (password != null)) {
            String base64credentials = BaseEncoding.base64().encode(
                    (username + ":" + password).getBytes(StandardCharsets.UTF_8));
            httpRequest.addHeader("Authorization", "Basic " + base64credentials);
        }

        try {
            CloseableHttpResponse response = _httpClient.execute(_httpHost, httpRequest);
            if (response.getEntity() != null) {
                return EntityUtils.toString(response.getEntity());
            }
            return null;
        } catch (ClientProtocolException e) {
            logger.error("An error occured while executing " + httpRequest, e);
            throw new IllegalStateException(e);
        } catch (IOException e) {
            logger.error("An error occured while executing " + httpRequest, e);
            throw new IllegalStateException(e);
        }
    }

    public String executeCypherQuery(String cypherQuery) {
        JSONObject cypherQueryRequest = new JSONObject();
        HashMap<String, String> statement = new HashMap<String, String>();
        statement.put("statement", cypherQuery);

        JSONArray statementsArray = new JSONArray();
        statementsArray.put(statement);

        return executeRequest(cypherQueryRequest, statementsArray);
    }

    public String executeCypherQueries(List<String> cypherQueries) {
        JSONObject cypherQueryRequest = new JSONObject();
        JSONArray statementsArray = new JSONArray();
        for (String cypherQuery : cypherQueries) {
            HashMap<String, String> statement = new HashMap<String, String>();
            statement.put("statement", cypherQuery);

            statementsArray.put(statement);
        }

        return executeRequest(cypherQueryRequest, statementsArray);
    }

    private String executeRequest(JSONObject cypherQueryRequest, JSONArray statementsArray) {
        try {
            cypherQueryRequest.put("statements", statementsArray);

            String requestBody = cypherQueryRequest.toString();
            _cypherQueryHttpPost.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));

            String responseJSONString = executeRestRequest(_cypherQueryHttpPost);
            return responseJSONString;
        } catch (JSONException e) {
            logger.error("Error occured while constructing JSON request body for " + _cypherQueryHttpPost, e);
            throw new IllegalStateException(e);
        }
    }

}

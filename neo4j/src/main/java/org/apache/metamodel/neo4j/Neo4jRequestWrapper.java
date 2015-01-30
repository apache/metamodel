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
import java.util.HashMap;

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

public class Neo4jRequestWrapper {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jRequestWrapper.class);
    
    private final CloseableHttpClient _httpClient;
    private final HttpHost _httpHost;
    private final HttpPost _httpPost;
    
    public Neo4jRequestWrapper(CloseableHttpClient httpClient, HttpHost httpHost) {
        _httpClient = httpClient;
        _httpHost = httpHost;
        _httpPost = new HttpPost("/db/data/transaction/commit"); 
    }
    
    public String executeRestRequest(HttpRequestBase httpRequest) {
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
        try {
            cypherQueryRequest.put("statements", statementsArray);
            
            String requestBody = cypherQueryRequest.toString();
            _httpPost.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_JSON));
            
            String responseJSONString = executeRestRequest(_httpPost);
            return responseJSONString;
        } catch (JSONException e) {
            logger.error("Error occured while constructing JSON request body for " + _httpPost, e);
            throw new IllegalStateException(e);
        }
        
    }
    
}

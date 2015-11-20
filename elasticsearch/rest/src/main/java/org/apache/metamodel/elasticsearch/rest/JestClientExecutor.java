/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.metamodel.elasticsearch.rest;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import org.apache.metamodel.MetaModelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class JestClientExecutor {
    private static final Logger logger = LoggerFactory.getLogger(JestClientExecutor.class);

    static <T extends JestResult> T execute(JestClient jestClient, Action<T> clientRequest) {
        return execute(jestClient, clientRequest, true);
    }

    static <T extends JestResult> T execute(JestClient jestClient, Action<T> clientRequest, boolean doThrow) {
        try {
            final T result = jestClient.execute(clientRequest);
            logger.debug("{} response: acknowledged={}", clientRequest, result.isSucceeded());
            return result;
        } catch (IOException e) {
            logger.warn("Could not execute command {} ", clientRequest, e);
            if (doThrow) {
                throw new MetaModelException("Could not execute command " + clientRequest, e);
            }
        }

        return null;
    }
}

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
package org.apache.metamodel.query.parser;

/**
 * Callback of the {@link QueryPartParser}, which recieves notifications
 * whenever a token is identified/parsed. A {@link QueryPartProcessor} is used
 * to perform the actual processing of identified tokens.
 */
public interface QueryPartProcessor {

    /**
     * Method invoked whenever the {@link QueryPartParser} identifies a token.
     * 
     * @param delim
     *            the (previous) delimitor identified before the token. This
     *            will always be null in case of the first token.
     * @param token
     *            the token identified.
     */
    public void parse(String delim, String token);
}

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

import org.apache.metamodel.MetaModelException;

/**
 * Subtype of {@link MetaModelException} which indicate a problem in parsing a
 * query passed to the {@link QueryParser}.
 */
public class QueryParserException extends MetaModelException {

    private static final long serialVersionUID = 1L;

    public QueryParserException() {
        super();
    }

    public QueryParserException(Exception cause) {
        super(cause);
    }

    public QueryParserException(String message, Exception cause) {
        super(message, cause);
    }

    public QueryParserException(String message) {
        super(message);
    }
}

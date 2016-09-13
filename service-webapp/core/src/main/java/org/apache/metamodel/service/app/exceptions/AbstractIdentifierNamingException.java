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
package org.apache.metamodel.service.app.exceptions;

import org.apache.metamodel.MetaModelException;

/**
 * Exception super class for any exception that arises because an identifier
 * (name, ID or such) is invalid for a specific context.
 */
public class AbstractIdentifierNamingException extends MetaModelException {

    private static final long serialVersionUID = 1L;
    private final String identifier;

    public AbstractIdentifierNamingException(String identifier) {
        super("Illegal value: " + identifier);
        this.identifier = identifier;
    }

    public String getIdentifier() {
        return identifier;
    }
}

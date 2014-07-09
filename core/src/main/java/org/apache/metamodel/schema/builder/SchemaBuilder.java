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
package org.apache.metamodel.schema.builder;

import org.apache.metamodel.convert.DocumentConverter;
import org.apache.metamodel.schema.MutableSchema;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

/**
 * Component that builds {@link Schema} objects.
 */
public interface SchemaBuilder {

    /**
     * Offers a {@link DocumentSourceProvider} to the {@link SchemaBuilder}. The
     * {@link SchemaBuilder} may consume the sources to build/detect a schema
     * based on the observed documents in the sources. It may also choose to
     * ignore the source, if the it does not need it.
     * 
     * @param documentSourceProvider
     */
    public void offerSources(DocumentSourceProvider documentSourceProvider);

    /**
     * Builds the {@link Schema}
     * 
     * @return
     */
    public MutableSchema build();

    /**
     * Gets a {@link DocumentConverter} for a table in the produced schema
     * 
     * @return
     */
    public DocumentConverter getDocumentConverter(Table table);

    /**
     * Gets the name of the schema that is built / will be built.
     * 
     * @return
     */
    public String getSchemaName();
}

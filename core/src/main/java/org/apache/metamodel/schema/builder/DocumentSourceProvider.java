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

import org.apache.metamodel.data.Document;
import org.apache.metamodel.data.DocumentSource;

/**
 * A provider of {@link DocumentSource}s for building schemas
 */
public interface DocumentSourceProvider {

    /**
     * Gets a {@link DocumentSource} containing documents of mixed origin and
     * type.
     * 
     * @return
     */
    public DocumentSource getMixedDocumentSourceForSampling();

    /**
     * Gets a {@link DocumentSource} for a particular source collection. See
     * {@link Document#getSourceCollectionName()}.
     * 
     * @param sourceCollectionName
     * @return
     */
    public DocumentSource getDocumentSourceForTable(String sourceCollectionName);
}

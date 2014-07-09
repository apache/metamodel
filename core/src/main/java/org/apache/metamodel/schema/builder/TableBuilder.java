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

import org.apache.metamodel.data.DocumentSource;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.MutableTable;
import org.apache.metamodel.schema.Table;

/**
 * Component that builds {@link Table} objects.
 */
public interface TableBuilder {

    /**
     * Offers a {@link DocumentSource} to the {@link TableBuilder}. The
     * {@link TableBuilder} may consume the source to build/detect a table based
     * on the observed documents in the source. It may also choose to ignore the
     * source, if the it does not need it.
     * 
     * @param documentSource
     */
    public void offerSource(DocumentSource documentSource);

    /**
     * Gets the {@link ColumnBuilder} of a named {@link Column} in the table.
     * 
     * @param columnName
     * @return
     */
    public ColumnBuilder getColumnBuilder(String columnName);

    /**
     * Builds the {@link Table}
     * 
     * @return
     */
    public MutableTable buildTable();
}

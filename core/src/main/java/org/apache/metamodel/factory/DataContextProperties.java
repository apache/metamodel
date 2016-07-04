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
package org.apache.metamodel.factory;

import java.net.URI;

import javax.sql.DataSource;

import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.SimpleTableDef;

public interface DataContextProperties {

    String getDataContextType();

    URI getUri();

    int getColumnNameLineNumber();

    boolean isSkipEmptyLines();

    boolean isSkipEmptyColumns();

    String getEncoding();

    char getSeparatorChar();

    char getQuoteChar();

    char getEscapeChar();

    boolean isFailOnInconsistentRowLength();

    boolean isMultilineValuesEnabled();

    TableType[] getTableTypes();

    String getCatalogName();

    DataSource getDataSource();

    String getUsername();

    String getPassword();

    String getDriverClassName();

    String getHostname();

    Integer getPort();

    String getDatabaseName();

    SimpleTableDef[] getTableDefs();
}

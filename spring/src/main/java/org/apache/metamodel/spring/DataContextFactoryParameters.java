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
package org.apache.metamodel.spring;

import javax.sql.DataSource;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.schema.TableType;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * Represents parameters used by a factory {@link DataContext} objects.
 * 
 * All getters in this interface may return null, since the parameters may be
 * originating from an external configuration file or similar means.
 */
public interface DataContextFactoryParameters {

    public abstract org.springframework.core.io.Resource getResource();

    public abstract String getFilename();

    public abstract String getUrl();

    public abstract String getColumnNameLineNumber();

    public abstract String getSkipEmptyLines();

    public abstract String getSkipEmptyColumns();

    public abstract String getEncoding();

    public abstract String getSeparatorChar();

    public abstract String getQuoteChar();

    public abstract String getEscapeChar();

    public abstract String getFailOnInconsistentRowLength();

    public abstract String getMultilineValues();

    public abstract TableType[] getTableTypes();

    public abstract String getCatalogName();

    public abstract DataSource getDataSource();

    public abstract String getUsername();

    public abstract String getPassword();

    public abstract String getDriverClassName();

    public abstract String getHostname();

    public abstract Integer getPort();

    public abstract String getDatabaseName();

    public abstract SimpleTableDef[] getTableDefs();

}
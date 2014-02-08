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

import org.apache.metamodel.DataContext;
import org.apache.metamodel.excel.ExcelConfiguration;
import org.apache.metamodel.excel.ExcelDataContext;
import org.apache.metamodel.util.Resource;

/**
 * {@link DataContextFactoryBeanDelegate} for {@link ExcelDataContext}.
 */
public class ExcelDataContextFactoryBeanDelegate extends AbstractDataContextFactoryBeanDelegate {

    @Override
    public DataContext createDataContext(DataContextFactoryParameters params) {
        final Resource resource = getResource(params);
        final int columnNameLineNumber = getInt(params.getColumnNameLineNumber(),
                ExcelConfiguration.DEFAULT_COLUMN_NAME_LINE);
        final boolean skipEmptyLines = getBoolean(params.getSkipEmptyLines(), true);
        final boolean skipEmptyColumns = getBoolean(params.getSkipEmptyColumns(), false);
        final ExcelConfiguration configuration = new ExcelConfiguration(columnNameLineNumber, skipEmptyLines,
                skipEmptyColumns);
        return new ExcelDataContext(resource, configuration);
    }
}

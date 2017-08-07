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
package org.apache.metamodel.fixedwidth;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.AbstractDataContextFactory;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Resource;

/**
 * {@link DataContextFactory} for {@link FixedWidthDataContext}s.
 * 
 * In addition to supporting common properties to define resource path, encoding etc., this implementation expects a
 * custom property called "columns". This property may take one of two forms:
 * 
 * <ul>
 * <li>A string format where column name and widths in characters are comma-separated. Example:
 * "id,5,name,10,amount,7,account,5".</li>
 * <li>A key/value map where keys are column names (strings) and values are column widths (numbers).</li>
 * </ul>
 */
public class FixedWidthDataContextFactory extends AbstractDataContextFactory {

    @Override
    protected String getType() {
        return "fixed-width";
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        assert accepts(properties, resourceFactoryRegistry);

        final Resource resource = resourceFactoryRegistry.createResource(properties.getResourceProperties());

        final String encoding = getString(properties.getEncoding(), FileHelper.DEFAULT_ENCODING);
        final boolean failOnInconsistentRowLength = getBoolean(properties.isFailOnInconsistentRowLength(), false);

        final List<FixedWidthColumnSpec> columnSpecs = new ArrayList<>();
        final Object columns = properties.toMap().get("columns");
        if (columns instanceof Map) {
            // expect a map with column names as keys, widths as values
            ((Map<String, Object>) columns).forEach((k, v) -> {
                final String name = k;
                final int width = ((Number) v).intValue();
                columnSpecs.add(new FixedWidthColumnSpec(name, width));
            });
        } else if (columns instanceof String) {
            final String[] split = ((String) columns).split(",");
            String nextName = null;
            for (String token : split) {
                try {
                    final int width = Integer.parseInt(token);
                    // parsing as a number worked - it must be a column width
                    columnSpecs.add(new FixedWidthColumnSpec(nextName, width));
                    nextName = null;
                } catch (NumberFormatException e) {
                    // parsing as a number didn't work - it must be a column
                    // name
                    nextName = token;
                }
            }
        }

        final FixedWidthConfiguration configuration =
                new FixedWidthConfiguration(encoding, columnSpecs, failOnInconsistentRowLength);
        return new FixedWidthDataContext(resource, configuration);
    }
}

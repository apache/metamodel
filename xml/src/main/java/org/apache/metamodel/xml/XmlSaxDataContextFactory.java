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
package org.apache.metamodel.xml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.metamodel.ConnectionException;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.factory.DataContextFactory;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.factory.ResourceFactoryRegistry;
import org.apache.metamodel.factory.UnsupportedDataContextPropertiesException;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.SimpleTableDef;

/**
 * A {@link DataContextFactory} for XML files where a "table-defs" or "xml-sax-table-defs" element has been configured
 * and represents the tables and their XPaths as defined by {@link XmlSaxDataContext}.
 * 
 * If "table-defs" is specified, it will first be interpreted as a {@link SimpleTableDef} which is then mapped to a
 * {@link XmlSaxTableDef}.
 * 
 * Alternatively, if "xml-sax-table-defs" is specified, it may either be a List, like this (JSON representation of lists
 * and maps):
 * 
 * <pre>
 * [
 *   {
 *     "row-x-path": "/path/to/entity1"
 *     "value-x-paths": [
 *       "/path/to/entity1/var1",
 *       "/path/to/entity1/var2"
 *     ]
 *   },
 *   {
 *     "row-x-path": "/path/to/model2"
 *     "value-x-paths": [
 *       "/path/to/model2/field3",
 *       "/path/to/model2/field4"
 *     ]
 *   }
 * ]
 * </pre>
 * 
 * Or a key/value map, like this:
 * 
 * <pre>
 *   {
 *     "/path/to/entity1": [
 *       "/path/to/entity1/var1",
 *       "/path/to/entity1/var2"
 *     ],
 *     "/path/to/model2": [
 *       "/path/to/model2/field3",
 *       "/path/to/model2/field4"
 *     ]
 *   }
 * </pre>
 */
public class XmlSaxDataContextFactory implements DataContextFactory {

    @Override
    public boolean accepts(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry) {
        final boolean hasTableDefs =
                properties.getTableDefs() != null || properties.toMap().containsKey("xml-sax-table-defs");
        return "xml".equals(properties.getDataContextType()) && hasTableDefs;
    }

    @Override
    public DataContext create(DataContextProperties properties, ResourceFactoryRegistry resourceFactoryRegistry)
            throws UnsupportedDataContextPropertiesException, ConnectionException {

        final List<XmlSaxTableDef> tableDefs;
        final SimpleTableDef[] simpleTableDefs = properties.getTableDefs();
        if (simpleTableDefs == null) {
            tableDefs = createTableDefFromProperties(properties.toMap().get("xml-sax-table-defs"));
        } else {
            tableDefs = Arrays.stream(simpleTableDefs).map(tableDef -> {
                return new XmlSaxTableDef(tableDef.getName(), tableDef.getColumnNames());
            }).collect(Collectors.toList());
        }

        final Resource resource = resourceFactoryRegistry.createResource(properties.getResourceProperties());
        return new XmlSaxDataContext(resource, tableDefs);
    }

    @SuppressWarnings("unchecked")
    private List<XmlSaxTableDef> createTableDefFromProperties(Object configuredTableDefs) {
        final List<XmlSaxTableDef> tableDefs = new ArrayList<>();

        if (configuredTableDefs instanceof List) {
            final List<Map<String, Object>> list = (List<Map<String, Object>>) configuredTableDefs;
            list.forEach(tableDef -> {
                final String rowXpath = (String) tableDef.get("row-x-path");
                final Collection<String> valueXpaths = (Collection<String>) tableDef.get("value-x-paths");
                tableDefs.add(new XmlSaxTableDef(rowXpath, valueXpaths));
            });
        } else if (configuredTableDefs instanceof Map) {
            final Map<String, Collection<String>> map = (Map<String, Collection<String>>) configuredTableDefs;
            map.forEach((k, v) -> {
                final String rowXpath = k;
                final Collection<String> valueXpaths = v;
                tableDefs.add(new XmlSaxTableDef(rowXpath, valueXpaths));
            });
        } else {
            throw new IllegalArgumentException(
                    "Unsupported value type for property 'xml-sax-table-defs'. Map or List expected, but got: "
                            + configuredTableDefs);
        }

        return tableDefs;
    }
}

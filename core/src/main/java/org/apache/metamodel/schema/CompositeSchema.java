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
package org.apache.metamodel.schema;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.metamodel.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A composite schema, comprising tables from several {@link DataContext}s.
 */
public class CompositeSchema extends AbstractSchema {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(CompositeSchema.class);

    private final String name;
    private final Collection<? extends Schema> delegates;

    public CompositeSchema(String name, Collection<? extends Schema> delegates) {
        super();
        this.name = name;
        this.delegates = delegates;
        if (logger.isWarnEnabled()) {
            Set<String> names = new HashSet<String>();
            for (Table table : getTables()) {
                if (names.contains(table.getName())) {
                    logger.warn("Name-clash detected for Table {}.", table.getName());
                    logger.warn("getTableByName(\"{}\") will return just the first table.", table.getName());
                } else {
                    names.add(table.getName());
                }
            }
            if (!names.isEmpty()) {
                logger.warn("The following table names clashes in composite schema: " + names);
            }
        }
    }

    @Override
    public List<Relationship> getRelationships() {
        return delegates.stream()
                .flatMap(delegate -> delegate.getRelationships().stream())
                .collect(Collectors.toList());
    }

    @Override
    public List<Table> getTables() {
        return delegates.stream()
                .flatMap(delegate -> delegate.getTables().stream())
                .collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getQuote() {
        return null;
    }
}
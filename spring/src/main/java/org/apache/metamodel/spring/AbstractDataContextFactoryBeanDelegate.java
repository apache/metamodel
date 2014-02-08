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

import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.util.BooleanComparator;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.UrlResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Useful common superclass of {@link DataContextFactoryBeanDelegate}
 * implementations.
 */
public abstract class AbstractDataContextFactoryBeanDelegate implements DataContextFactoryBeanDelegate {
    
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected Resource getResource(DataContextFactoryParameters params) {
        final org.springframework.core.io.Resource resource = params.getResource();
        if (resource != null) {
            return new SpringResource(resource);
        } else if (params.getFilename() != null) {
            return new FileResource(params.getFilename());
        } else if (params.getUrl() != null) {
            return new UrlResource(params.getUrl());
        }
        return null;
    }

    protected int getInt(String value, int ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        return Integer.parseInt(value);
    }

    protected String getString(String value, String ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        return value;
    }

    protected char getChar(String value, char ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        if ("none".equalsIgnoreCase(value)) {
            return CsvConfiguration.NOT_A_CHAR;
        }
        return value.charAt(0);
    }

    protected boolean getBoolean(String value, boolean ifNull) {
        if (value == null) {
            return ifNull;
        }
        value = value.trim();
        if (value.isEmpty()) {
            return ifNull;
        }
        return BooleanComparator.parseBoolean(value);
    }
}

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.metamodel.util.AbstractResource;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Resource} implementation based on spring's similar
 * {@link org.springframework.core.io.Resource} concept.
 */
public class SpringResource extends AbstractResource implements Resource {

    private static final Logger logger = LoggerFactory.getLogger(SpringResource.class);

    private final org.springframework.core.io.Resource _resource;

    public SpringResource(org.springframework.core.io.Resource resource) {
        _resource = resource;
    }
    
    @Override
    public OutputStream append() throws ResourceException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public OutputStream write() throws ResourceException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public long getLastModified() {
        try {
            return _resource.lastModified();
        } catch (IOException e) {
            logger.warn("Failed to get last modified date of resource: " + _resource, e);
            return -1;
        }
    }

    @Override
    public String getName() {
        return _resource.getFilename();
    }

    @Override
    public String getQualifiedPath() {
        try {
            return _resource.getURI().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get URI of resource: " + _resource, e);
        }
    }

    @Override
    public long getSize() {
        try {
            return _resource.contentLength();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get content length of resource: " + _resource, e);
        }
    }

    @Override
    public boolean isExists() {
        return _resource.exists();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public InputStream read() throws ResourceException {
        try {
            return _resource.getInputStream();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get input stream of resource: " + _resource, e);
        }
    }

    /**
     * Gets the underlying spring {@link org.springframework.core.io.Resource}
     * object.
     * 
     * @return
     */
    public org.springframework.core.io.Resource getResource() {
        return _resource;
    }
}

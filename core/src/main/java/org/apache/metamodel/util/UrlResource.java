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
package org.apache.metamodel.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Resource based on URL or URI.
 */
public class UrlResource extends AbstractResource implements Serializable {

    private static final long serialVersionUID = 1L;

    private final URI _uri;

    public UrlResource(URL url) {
        try {
            _uri = url.toURI();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public UrlResource(URI uri) {
        _uri = uri;
    }

    public UrlResource(String urlString) {
        try {
            _uri = new URI(urlString);
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return "UrlResource[" + _uri + "]";
    }

    /**
     * Gets the URI associated with this resource.
     * 
     * @return
     */
    public URI getUri() {
        return _uri;
    }

    @Override
    public String getName() {
        final String name = _uri.toString();
        final int lastSlash = name.lastIndexOf('/');
        final int lastBackSlash = name.lastIndexOf('\\');
        final int lastIndex = Math.max(lastSlash, lastBackSlash);
        if (lastIndex != -1) {
            final String lastPart = name.substring(lastIndex + 1);
            if (!"".equals(lastPart)) {
                return lastPart;
            }
        }
        return name;
    }

    @Override
    public String getQualifiedPath() {
        return _uri.toString();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public OutputStream write() throws ResourceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream append() throws ResourceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isExists() {
        return true;
    }

    @Override
    public long getSize() {
        return -1;
    }

    @Override
    public long getLastModified() {
        return -1;
    }

    @Override
    public InputStream read() throws ResourceException {
        try {
            return _uri.toURL().openStream();
        } catch (Exception e) {
            throw new ResourceException(this, "Failed to open InputStream", e);
        }
    }

}

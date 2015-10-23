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
import java.net.URL;

/**
 * A {@link Resource} based on a classpath entry
 */
public class ClasspathResource extends AbstractResource implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String _resourcePath;

    public ClasspathResource(String resourcePath) {
        if (resourcePath == null) {
            throw new IllegalArgumentException("Classpath resource path cannot be null");
        }
        _resourcePath = resourcePath;
    }

    @Override
    public String toString() {
        return "ClasspathResource[" + _resourcePath + "]";
    }

    /**
     * Gets the name of the classpath entry
     *
     * @return
     */
    public String getResourcePath() {
        return _resourcePath;
    }

    @Override
    public String getName() {
        String name = _resourcePath;
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
        return _resourcePath;
    }

    protected ClassLoader getClassLoader() {
        return ClassLoader.getSystemClassLoader();
    }

    private UrlResource getUrlResourceDelegate() {
        ClassLoader classLoader = getClassLoader();

        URL url = classLoader.getResource(_resourcePath);
        if (url == null && _resourcePath.startsWith("/")) {
            url = classLoader.getResource(_resourcePath.substring(1));
        }

        if (url == null) {
            return null;
        }
        return new UrlResource(url);
    }
    
    @Override
    public OutputStream append() throws ResourceException {
        return getUrlResourceDelegate().append();
    }
    
    @Override
    public OutputStream write() throws ResourceException {
        return getUrlResourceDelegate().write();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean isExists() {
        UrlResource delegate = getUrlResourceDelegate();
        if (delegate == null) {
            return false;
        }
        return delegate.isExists();
    }

    @Override
    public long getSize() {
        UrlResource delegate = getUrlResourceDelegate();
        if (delegate == null) {
            return -1;
        }
        return delegate.getSize();
    }

    @Override
    public long getLastModified() {
        UrlResource delegate = getUrlResourceDelegate();
        if (delegate == null) {
            return -1;
        }
        return delegate.getLastModified();
    }

    @Override
    public InputStream read() throws ResourceException {
        return getUrlResourceDelegate().read();
    }

}

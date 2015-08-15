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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * An entirely in-memory kept {@link Resource}.
 */
public class InMemoryResource extends AbstractResource implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String _path;
    private byte[] _contents;
    private long _lastModified;

    /**
     * Constructs a new {@link InMemoryResource} with a path/name
     * 
     * @param path
     */
    public InMemoryResource(String path) {
        this(path, new byte[0], -1);
    }

    /**
     * Constructs a new {@link InMemoryResource} with a path/name and some
     * initial contents.
     * 
     * @param path
     * @param contents
     * @param lastModified
     */
    public InMemoryResource(String path, byte[] contents, long lastModified) {
        _path = path;
        _contents = contents;
        _lastModified = lastModified;
    }

    @Override
    public String toString() {
        return "InMemoryResource[" + _path + "]";
    }

    @Override
    public String getName() {
        String name = _path;
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
        return _path;
    }

    /**
     * Gets the path of this resource
     * 
     * @return
     */
    public String getPath() {
        return _path;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isExists() {
        return true;
    }

    @Override
    public long getSize() {
        return _contents.length;
    }

    @Override
    public long getLastModified() {
        return _lastModified;
    }

    @Override
    public OutputStream write() throws ResourceException {
        return createOutputStream(false);
    }

    @Override
    public OutputStream append() throws ResourceException {
        return createOutputStream(true);
    }

    private OutputStream createOutputStream(boolean append) throws ResourceException {
        ByteArrayOutputStream out = new ByteArrayOutputStream() {
            @Override
            public void flush() throws IOException {
                super.flush();
                _contents = toByteArray();
                _lastModified = System.currentTimeMillis();
            }
        };
        if (append) {
            try {
                out.write(_contents);
            } catch (IOException e) {
                throw new ResourceException(this, e);
            }
        }
        return out;
    }

    @Override
    public InputStream read() throws ResourceException {
        return new ByteArrayInputStream(_contents);
    }
}

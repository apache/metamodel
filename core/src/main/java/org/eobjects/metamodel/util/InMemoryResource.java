/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * An entirely in-memory kept {@link Resource}.
 */
public class InMemoryResource implements Resource, Serializable {

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
    public void write(Action<OutputStream> writeCallback) throws ResourceException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            writeCallback.run(baos);
            _contents = baos.toByteArray();
            _lastModified = System.currentTimeMillis();
        } catch (Exception e) {
            throw new ResourceException(this, e);
        }
    }

    @Override
    public void append(Action<OutputStream> appendCallback) throws ResourceException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(_contents);
            appendCallback.run(baos);
            _contents = baos.toByteArray();
            _lastModified = System.currentTimeMillis();
        } catch (Exception e) {
            throw new ResourceException(this, e);
        }
    }

    @Override
    public InputStream read() throws ResourceException {
        return new ByteArrayInputStream(_contents);
    }

    @Override
    public void read(Action<InputStream> readCallback) throws ResourceException {
        final InputStream inputStream = read();
        try {
            readCallback.run(inputStream);
        } catch (Exception e) {
            throw new ResourceException(this, e);
        }
    }

    @Override
    public <E> E read(Func<InputStream, E> readCallback) throws ResourceException {
        final InputStream inputStream = read();
        try {
            return readCallback.eval(inputStream);
        } catch (Exception e) {
            throw new ResourceException(this, e);
        }
    }

}

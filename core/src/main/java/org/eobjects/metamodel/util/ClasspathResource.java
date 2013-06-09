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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URL;

/**
 * A {@link Resource} based on a classpath entry
 */
public class ClasspathResource implements Resource, Serializable {

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
    public void write(Action<OutputStream> writeCallback) throws ResourceException {
        getUrlResourceDelegate().write(writeCallback);
    }

    @Override
    public void append(Action<OutputStream> appendCallback) throws ResourceException {
        getUrlResourceDelegate().append(appendCallback);
    }

    @Override
    public InputStream read() throws ResourceException {
        return getUrlResourceDelegate().read();
    }

    @Override
    public void read(Action<InputStream> readCallback) throws ResourceException {
        getUrlResourceDelegate().read(readCallback);
    }

    @Override
    public <E> E read(Func<InputStream, E> readCallback) throws ResourceException {
        return getUrlResourceDelegate().read(readCallback);
    }

}

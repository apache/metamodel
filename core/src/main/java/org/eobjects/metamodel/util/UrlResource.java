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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Resource based on URL or URI.
 */
public class UrlResource implements Resource, Serializable {

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
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void write(Action<OutputStream> writeCallback) throws ResourceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void append(Action<OutputStream> appendCallback) throws ResourceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void read(Action<InputStream> readCallback) throws ResourceException {
        final InputStream in = read();
        try {
            readCallback.run(in);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }

    @Override
    public <E> E read(Func<InputStream, E> readCallback) throws ResourceException {
        final InputStream in = read();
        try {
            E result = readCallback.eval(in);
            return result;
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
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

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

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * {@link File} based {@link Resource} implementation.
 */
public class FileResource implements Resource, Serializable {

    private static final long serialVersionUID = 1L;
    private final File _file;

    public FileResource(String filename) {
        _file = new File(filename);
    }

    public FileResource(File file) {
        _file = file;
    }
    
    @Override
    public String toString() {
        return "FileResource[" + _file.getPath() + "]";
    }

    @Override
    public String getName() {
        return _file.getName();
    }

    @Override
    public boolean isReadOnly() {
        if (!isExists()) {
            return false;
        }
        boolean canWrite = _file.canWrite();
        return !canWrite;
    }

    @Override
    public void write(Action<OutputStream> writeCallback) throws ResourceException {
        final OutputStream out = FileHelper.getOutputStream(_file);
        try {
            writeCallback.run(out);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in write callback", e);
        } finally {
            FileHelper.safeClose(out);
        }
    }

    @Override
    public void append(Action<OutputStream> appendCallback) {
        final OutputStream out = FileHelper.getOutputStream(_file, true);
        try {
            appendCallback.run(out);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in append callback", e);
        } finally {
            FileHelper.safeClose(out);
        }
    }

    @Override
    public void read(Action<InputStream> readCallback) {
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
    public <E> E read(Func<InputStream, E> readCallback) {
        final InputStream in = read();
        try {
            final E result = readCallback.eval(in);
            return result;
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }

    public File getFile() {
        return _file;
    }

    @Override
    public boolean isExists() {
        return _file.exists();
    }

    @Override
    public long getSize() {
        return _file.length();
    }

    @Override
    public long getLastModified() {
        final long lastModified = _file.lastModified();
        if (lastModified == 0) {
            return -1;
        }
        return lastModified;
    }

    @Override
    public InputStream read() throws ResourceException {
        final InputStream in = FileHelper.getInputStream(_file);
        return in;
    }
}

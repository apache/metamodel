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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * {@link File} based {@link Resource} implementation.
 */
public class FileResource implements Resource, Serializable {

    private class DirectoryInputStream extends AbstractDirectoryInputStream<File> {

        public DirectoryInputStream() {
            final File[] unsortedFiles = _file.listFiles(new FileFilter() {
                @Override
                public boolean accept(final File pathname) {
                    return pathname.isFile();
                }
            });

            if (unsortedFiles == null) {
                _files = new File[0];
            } else {
                Arrays.sort(unsortedFiles);
                _files = unsortedFiles;
            }
        }

        @Override
        InputStream openStream(final int index) throws IOException {
            return FileHelper.getInputStream(_files[index]);
        }
    }

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
    public String getQualifiedPath() {
        try {
            return _file.getCanonicalPath();
        } catch (IOException e) {
            return _file.getAbsolutePath();
        }
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
        if (_file.isDirectory()) {
            return new DirectoryInputStream();
        }
        final InputStream in = FileHelper.getInputStream(_file);
        return in;
    }
}

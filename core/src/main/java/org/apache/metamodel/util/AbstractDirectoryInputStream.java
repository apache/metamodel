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

import java.io.IOException;
import java.io.InputStream;

public abstract class AbstractDirectoryInputStream<T> extends InputStream {
    protected T[] _files;
    private int _currentFileIndex = -1;
    private InputStream _currentInputStream;


    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (_currentInputStream != null) {
            final int byteCount = _currentInputStream.read(b, off, len);
            if (byteCount > 0) {
                return byteCount;
            }
        }

        if (!openNextFile()) {
            return -1; // No more files.
        }

        return read(b, off, len);
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read() throws IOException {
        final byte[] b = new byte[1];
        int count = read(b, 0, 1);
        if (count < 0) {
            return -1;
        }
        return (int) b[0];
    }

    @Override
    public int available() throws IOException {
        if (_currentInputStream != null) {
            return _currentInputStream.available();
        } else {
            return 0;
        }
    }

    private boolean openNextFile() throws IOException {
        if (_currentInputStream != null) {
            FileHelper.safeClose(_currentInputStream);
            _currentInputStream = null;
        }
        _currentFileIndex++;
        if (_currentFileIndex >= _files.length) {
            return false;
        }

        _currentInputStream = openStream(_currentFileIndex);
        return true;
    }

    abstract InputStream openStream(int index) throws IOException;

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (_currentInputStream != null) {
            FileHelper.safeClose(_currentInputStream);
        }
    }
}

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

import org.apache.hadoop.fs.FileSystem;

/**
 * A managed {@link InputStream} for a file on HDFS.
 * 
 * The "purpose in life" for this class is to ensure that the {@link FileSystem}
 * is closed when the stream is closed.
 */
class HdfsFileInputStream extends InputStream {

    private final InputStream _in;
    private final FileSystem _fs;

    public HdfsFileInputStream(final InputStream in, final FileSystem fs) {
        _in = in;
        _fs = fs;
    }

    @Override
    public int read() throws IOException {
        return _in.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return _in.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return _in.read(b);
    }

    @Override
    public boolean markSupported() {
        return _in.markSupported();
    }

    @Override
    public synchronized void mark(int readLimit) {
        _in.mark(readLimit);
    }

    @Override
    public int available() throws IOException {
        return _in.available();
    }

    @Override
    public synchronized void reset() throws IOException {
        _in.reset();
    }

    @Override
    public long skip(long n) throws IOException {
        return _in.skip(n);
    }

    @Override
    public void close() throws IOException {
        super.close();
        // need to close 'fs' when input stream is closed
        FileHelper.safeClose(_fs);
    }
}

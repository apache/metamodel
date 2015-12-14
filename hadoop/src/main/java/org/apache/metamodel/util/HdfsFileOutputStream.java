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
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;

/**
 * A managed {@link OutputStream} for a file on HDFS.
 * 
 * The "purpose in life" for this class is to ensure that the {@link FileSystem}
 * is closed when the stream is closed.
 */
class HdfsFileOutputStream extends OutputStream {

    private final OutputStream _out;
    private final FileSystem _fs;

    public HdfsFileOutputStream(final OutputStream out, final FileSystem fs) {
        _out = out;
        _fs = fs;
    }

    @Override
    public void write(int b) throws IOException {
        _out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        _out.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        _out.write(b);
    }

    @Override
    public void flush() throws IOException {
        _out.flush();
    }

    @Override
    public void close() throws IOException {
        super.close();
        // need to close 'fs' when output stream is closed
        FileHelper.safeClose(_fs);
    }
}

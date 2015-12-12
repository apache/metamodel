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
import java.util.Arrays;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * An {@link InputStream} that represents all the data found in a directory on
 * HDFS. This {@link InputStream} is used by {@link HdfsResource#read()} when
 * pointed to a directory.
 */
class HdfsDirectoryInputStream extends AbstractDirectoryInputStream<FileStatus> {

    private final Path _hadoopPath;
    private final FileSystem _fs;

    public HdfsDirectoryInputStream(final Path hadoopPath, final FileSystem fs) {
        _hadoopPath = hadoopPath;
        _fs = fs;
        FileStatus[] fileStatuses;
        try {
            fileStatuses = _fs.listStatus(_hadoopPath, new PathFilter() {
                @Override
                public boolean accept(final Path path) {
                    try {
                        return _fs.isFile(path);
                    } catch (IOException e) {
                        return false;
                    }
                }
            });
            // Natural ordering is the URL
            Arrays.sort(fileStatuses);
        } catch (IOException e) {
            fileStatuses = new FileStatus[0];
        }
        _files = fileStatuses;
    }

    @Override
    public InputStream openStream(final int index) throws IOException {
        final Path nextPath = _files[index].getPath();
        return _fs.open(nextPath);
    }

    @Override
    public void close() throws IOException {
        super.close();
        FileHelper.safeClose(_fs);
    }
}

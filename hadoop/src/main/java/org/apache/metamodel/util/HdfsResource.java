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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.metamodel.MetaModelException;

import com.google.common.base.Strings;

/**
 * A {@link Resource} implementation that connects to Apache Hadoop's HDFS
 * distributed file system.
 */
public class HdfsResource extends AbstractResource implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Pattern URL_PATTERN = Pattern.compile("hdfs://(.+):([0-9]+)/(.*)");

    private final String _hadoopConfDir;
    private final String _hostname;
    private final int _port;
    private final String _filepath;
    private transient Path _path;

    /**
     * Creates a {@link HdfsResource}
     *
     * @param url
     *            a URL of the form: hdfs://hostname:port/path/to/file
     */
    public HdfsResource(String url) {
        this(url, null);
    }

    /**
     * Creates a {@link HdfsResource}
     *
     * @param url
     *            a URL of the form: hdfs://hostname:port/path/to/file
     * @param hadoopConfDir
     *            the path of a directory containing the Hadoop and HDFS
     *            configuration file(s).
     */
    public HdfsResource(String url, String hadoopConfDir) {
        if (url == null) {
            throw new IllegalArgumentException("Url cannot be null");
        }
        final Matcher matcher = URL_PATTERN.matcher(url);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    "Cannot parse url '" + url + "'. Must follow pattern: hdfs://hostname:port/path/to/file");
        }
        _hostname = matcher.group(1);
        _port = Integer.parseInt(matcher.group(2));
        _filepath = '/' + matcher.group(3);
        _hadoopConfDir = hadoopConfDir;
    }

    /**
     * Creates a {@link HdfsResource}
     *
     * @param hostname
     *            the HDFS (namenode) hostname
     * @param port
     *            the HDFS (namenode) port number
     * @param filepath
     *            the path on HDFS to the file, starting with slash ('/')
     */
    public HdfsResource(String hostname, int port, String filepath) {
        this(hostname, port, filepath, null);
    }

    /**
     * Creates a {@link HdfsResource}
     *
     * @param hostname
     *            the HDFS (namenode) hostname
     * @param port
     *            the HDFS (namenode) port number
     * @param filepath
     *            the path on HDFS to the file, starting with slash ('/')
     * @param hadoopConfDir
     *            the path of a directory containing the Hadoop and HDFS
     *            configuration file(s).
     */
    public HdfsResource(String hostname, int port, String filepath, String hadoopConfDir) {
        _hostname = hostname;
        _port = port;
        _filepath = filepath;
        _hadoopConfDir = hadoopConfDir;
    }

    public String getFilepath() {
        return _filepath;
    }

    public String getHostname() {
        return _hostname;
    }

    public int getPort() {
        return _port;
    }

    public String getHadoopConfDir() {
        return _hadoopConfDir;
    }

    @Override
    public String getName() {
        final int lastSlash = _filepath.lastIndexOf('/');
        if (lastSlash != -1) {
            return _filepath.substring(lastSlash + 1);
        }
        return _filepath;
    }

    @Override
    public String getQualifiedPath() {
        return "hdfs://" + _hostname + ":" + _port + _filepath;
    }

    @Override
    public boolean isReadOnly() {
        // We assume it is not read-only
        return false;
    }

    @Override
    public boolean isExists() {
        final FileSystem fs = getHadoopFileSystem();
        try {
            return fs.exists(getHadoopPath());
        } catch (Exception e) {
            throw wrapException(e);
        } finally {
            FileHelper.safeClose(fs);
        }
    }

    @Override
    public long getSize() {
        final FileSystem fs = getHadoopFileSystem();
        try {
            if (fs.isFile(getHadoopPath())) {
                return fs.getFileStatus(getHadoopPath()).getLen();
            } else {
                return fs.getContentSummary(getHadoopPath()).getLength();
            }
        } catch (Exception e) {
            throw wrapException(e);
        } finally {
            FileHelper.safeClose(fs);
        }
    }

    @Override
    public long getLastModified() {
        final FileSystem fs = getHadoopFileSystem();
        try {
            return fs.getFileStatus(getHadoopPath()).getModificationTime();
        } catch (Exception e) {
            throw wrapException(e);
        } finally {
            FileHelper.safeClose(fs);
        }
    }

    @Override
    public OutputStream write() throws ResourceException {
        final FileSystem fs = getHadoopFileSystem();
        try {
            final FSDataOutputStream out = fs.create(getHadoopPath(), true);
            return new HdfsFileOutputStream(out, fs);
        } catch (IOException e) {
            // we can close 'fs' in case of an exception
            FileHelper.safeClose(fs);
            throw wrapException(e);
        }
    }

    @Override
    public OutputStream append() throws ResourceException {
        final FileSystem fs = getHadoopFileSystem();
        try {
            final FSDataOutputStream out = fs.append(getHadoopPath());
            return new HdfsFileOutputStream(out, fs);
        } catch (IOException e) {
            // we can close 'fs' in case of an exception
            FileHelper.safeClose(fs);
            throw wrapException(e);
        }
    }

    @Override
    public InputStream read() throws ResourceException {
        final FileSystem fs = getHadoopFileSystem();
        final InputStream in;
        try {
            final Path hadoopPath = getHadoopPath();
            // return a wrapper InputStream which manages the 'fs' closeable
            if (fs.isFile(hadoopPath)) {
                in = fs.open(hadoopPath);
                return new HdfsFileInputStream(in, fs);
            } else {
                return new HdfsDirectoryInputStream(hadoopPath, fs);
            }
        } catch (Exception e) {
            // we can close 'fs' in case of an exception
            FileHelper.safeClose(fs);
            throw wrapException(e);
        }
    }

    private RuntimeException wrapException(Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        }
        return new MetaModelException(e);
    }

    public Configuration getHadoopConfiguration() {
        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + _hostname + ":" + _port);

        final File hadoopConfigurationDirectory = getHadoopConfigurationDirectoryToUse();
        if (hadoopConfigurationDirectory != null) {
            addResourceIfExists(conf, hadoopConfigurationDirectory, "core-site.xml");
            addResourceIfExists(conf, hadoopConfigurationDirectory, "hdfs-site.xml");
        }

        return conf;
    }

    private void addResourceIfExists(Configuration conf, File hadoopConfigurationDirectory, String filename) {
        final File file = new File(hadoopConfigurationDirectory, filename);
        if (file.exists()) {
            final InputStream inputStream = FileHelper.getInputStream(file);
            conf.addResource(inputStream, filename);
        }
    }

    private File getHadoopConfigurationDirectoryToUse() {
        File candidate = getDirectoryIfExists(_hadoopConfDir);
        if (candidate == null) {
            candidate = getDirectoryIfExists(System.getenv("YARN_CONF_DIR"));
            if (candidate == null) {
                candidate = getDirectoryIfExists(System.getenv("HADOOP_CONF_DIR"));
            }
        }
        return candidate;
    }

    private File getDirectoryIfExists(String path) {
        if (!Strings.isNullOrEmpty(path)) {
            final File directory = new File(path);
            if (directory.exists() && directory.isDirectory()) {
                return directory;
            }
        }
        return null;
    }

    public FileSystem getHadoopFileSystem() {
        try {
            return FileSystem.newInstance(getHadoopConfiguration());
        } catch (IOException e) {
            throw new MetaModelException("Could not connect to HDFS: " + e.getMessage(), e);
        }
    }

    public Path getHadoopPath() {
        if (_path == null) {
            _path = new Path(_filepath);
        }
        return _path;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_filepath, _hostname, _port, _hadoopConfDir);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof HdfsResource) {
            final HdfsResource other = (HdfsResource) obj;
            return Objects.equals(_filepath, other._filepath) && Objects.equals(_hostname, other._hostname)
                    && Objects.equals(_port, other._port) && Objects.equals(_hadoopConfDir, other._hadoopConfDir);
        }
        return false;
    }
}

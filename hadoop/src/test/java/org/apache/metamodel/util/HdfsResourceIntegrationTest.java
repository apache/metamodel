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
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class HdfsResourceIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(HdfsResourceIntegrationTest.class);

    private String _filePath;
    private String _hostname;
    private int _port;

    @Before
    public void setUp() throws Exception {
        final File file = new File(getPropertyFilePath());
        final boolean configured;
        if (file.exists()) {
            final Properties properties = new Properties();
            properties.load(new FileReader(file));
            _filePath = properties.getProperty("hadoop.hdfs.file.path");
            _hostname = properties.getProperty("hadoop.hdfs.hostname");
            final String portString = properties.getProperty("hadoop.hdfs.port");
            configured = _filePath != null && _hostname != null && portString != null;
            if (configured) {
                _port = Integer.parseInt(portString);
            } else {
                System.out.println("Skipping test because HDFS file path, hostname and port is not set");
            }
        } else {
            System.out.println("Skipping test because properties file does not exist");
            configured = false;
        }
        Assume.assumeTrue(configured);
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    @Test
    public void testReadDirectory() throws Exception {
        final String contentString = "fun and games with Apache MetaModel and Hadoop is what we do";
        final String[] contents = new String[] { "fun ", "and ", "games ", "with ", "Apache ", "MetaModel ", "and ",
                "Hadoop ", "is ", "what ", "we ", "do" };

        final Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + _hostname + ":" + _port);
        final FileSystem fileSystem = FileSystem.get(conf);
        final Path path = new Path(_filePath);
        final boolean exists = fileSystem.exists(path);

        if (exists) {
            fileSystem.delete(path, true);
        }

        fileSystem.mkdirs(path);

        // Reverse both filename and contents to make sure it is the name and
        // not the creation order that is sorted on.
        int i = contents.length;
        Collections.reverse(Arrays.asList(contents));
        for (final String contentPart : contents) {
            final HdfsResource partResource = new HdfsResource(_hostname, _port, _filePath + "/part-" + String.format(
                    "%02d", i--));
            partResource.write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream out) throws Exception {
                    out.write(contentPart.getBytes());
                }
            });
        }

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final HdfsResource res1 = new HdfsResource(_hostname, _port, _filePath);
        try {
            logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - start");

            final String str1 = res1.read(in -> {
                return FileHelper.readInputStreamAsString(in, "UTF8");
            });

            Assert.assertEquals(contentString, str1);
            logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - read1");

            final String str2 = res1.read(in -> {
                return FileHelper.readInputStreamAsString(in, "UTF8");
            });
            Assert.assertEquals(str1, str2);
            logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - read2");

            final StringBuilder sb = new StringBuilder();
            for (String token : contents) {
                sb.append(token);
            }
            final long expectedSize = sb.length();
            Assert.assertEquals(expectedSize, res1.getSize());
            logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - getSize");

            Assert.assertTrue(res1.getLastModified() > System.currentTimeMillis() - 10000);
            logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - getLastModified");

        } finally {
            res1.getHadoopFileSystem().delete(res1.getHadoopPath(), true);
            logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - deleted");
        }

        Assert.assertFalse(res1.isExists());

        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - done");
        stopwatch.stop();
    }

    @Test
    public void testReadOnRealHdfsInstall() throws Exception {
        final String contentString = "fun and games with Apache MetaModel and Hadoop is what we do";

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final HdfsResource res1 = new HdfsResource(_hostname, _port, _filePath);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - start");

        res1.write(new Action<OutputStream>() {
            @Override
            public void run(OutputStream out) throws Exception {
                out.write(contentString.getBytes());
            }
        });

        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - written");

        Assert.assertTrue(res1.isExists());

        final String str1 = res1.read(in -> {
            return FileHelper.readInputStreamAsString(in, "UTF8");
        });
        Assert.assertEquals(contentString, str1);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - read1");

        final String str2 = res1.read(in -> {
            return FileHelper.readInputStreamAsString(in, "UTF8");
        });
        Assert.assertEquals(str1, str2);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - read2");

        res1.getHadoopFileSystem().delete(res1.getHadoopPath(), false);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - deleted");

        Assert.assertFalse(res1.isExists());

        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - done");
        stopwatch.stop();
    }

    @Test
    public void testFileSystemNotBeingClosed() throws Throwable {
        HdfsResource resourceToRead = null;
        try {
            resourceToRead = new HdfsResource(_hostname, _port, _filePath);
            resourceToRead.write(new Action<OutputStream>() {

                @Override
                public void run(OutputStream out) throws Exception {
                    FileHelper.writeString(out, "testFileSystemNotBeingClosed");
                }
            });

            final MutableRef<Throwable> throwableRef = new MutableRef<>();

            Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread th, Throwable ex) {
                    throwableRef.set(ex);
                    Assert.fail("Caught exception in the thread: " + ex);
                }
            };

            Thread[] threads = new Thread[10];

            for (int i = 0; i < threads.length; i++) {
                final HdfsResource res = new HdfsResource(_hostname, _port, _filePath);

                Thread thread = new Thread(new Runnable() {

                    @Override
                    public void run() {
                        res.read(new Action<InputStream>() {

                            @Override
                            public void run(InputStream is) throws Exception {
                                String readAsString = FileHelper.readAsString(FileHelper.getReader(is, "UTF-8"));
                                Assert.assertNotNull(readAsString);
                                Assert.assertEquals("testFileSystemNotBeingClosed", readAsString);
                            }
                        });

                    }
                });

                thread.setUncaughtExceptionHandler(exceptionHandler);
                thread.start();
                threads[i] = thread;
            }

            for (int i = 0; i < threads.length; i++) {
                threads[i].join();
            }

            Throwable error = throwableRef.get();
            if (error != null) {
                throw error;
            }

        } finally {
            if (resourceToRead != null) {
                final FileSystem fileSystem = resourceToRead.getHadoopFileSystem();
                final Path resourceToReadPath = new Path(resourceToRead.getFilepath());
                if (fileSystem.exists(resourceToReadPath)) {
                    fileSystem.delete(resourceToReadPath, true);
                }
            }
        }
    }
}

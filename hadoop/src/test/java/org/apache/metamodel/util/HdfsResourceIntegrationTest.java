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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import junit.framework.TestCase;

public class HdfsResourceIntegrationTest extends TestCase {
    
    private static final Logger logger = LoggerFactory.getLogger(HdfsResourceIntegrationTest.class);

    private boolean _configured;
    private Properties _properties;
    private String _filePath;
    private String _hostname;
    private int _port;

    @Override
    protected final void setUp() throws Exception {
        super.setUp();

        _properties = new Properties();
        final File file = new File(getPropertyFilePath());
        if (file.exists()) {
            _properties.load(new FileReader(file));
            _filePath = _properties.getProperty("hadoop.hdfs.file.path");
            _hostname = _properties.getProperty("hadoop.hdfs.hostname");
            final String portString = _properties.getProperty("hadoop.hdfs.port");
            _configured = _filePath != null && _hostname != null && portString != null;
            if (_configured) {
                _port = Integer.parseInt(portString);
            }
        } else {
            _configured = false;
        }
    }

    private String getPropertyFilePath() {
        String userHome = System.getProperty("user.home");
        return userHome + "/metamodel-integrationtest-configuration.properties";
    }

    public void testReadOnRealHdfsInstall() throws Exception {
        if (!_configured) {
            System.err.println("!!! WARN !!! Hadoop HDFS integration test ignored\r\n"
                    + "Please configure Hadoop HDFS test-properties (" + getPropertyFilePath()
                    + "), to run integration tests");
            return;
        }
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

        assertTrue(res1.isExists());

        final String str1 = res1.read(new Func<InputStream, String>() {
            @Override
            public String eval(InputStream in) {
                return FileHelper.readInputStreamAsString(in, "UTF8");
            }
        });
        assertEquals(contentString, str1);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - read1");

        final String str2 = res1.read(new Func<InputStream, String>() {
            @Override
            public String eval(InputStream in) {
                return FileHelper.readInputStreamAsString(in, "UTF8");
            }
        });
        assertEquals(str1, str2);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - read2");

        res1.getHadoopFileSystem().delete(res1.getHadoopPath(), false);
        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - deleted");

        assertFalse(res1.isExists());

        logger.info(stopwatch.elapsed(TimeUnit.MILLISECONDS) + " - done");
        stopwatch.stop();
    }
}

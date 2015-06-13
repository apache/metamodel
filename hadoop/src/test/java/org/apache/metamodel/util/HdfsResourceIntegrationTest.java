package org.apache.metamodel.util;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import junit.framework.TestCase;

public class HdfsResourceIntegrationTest extends TestCase {

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
        final HdfsResource res1 = new HdfsResource(_hostname, _port, _filePath);
        try {
            assertFalse(res1.isExists());

            res1.write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream out) throws Exception {
                    out.write(contentString.getBytes());
                }
            });

            assertTrue(res1.isExists());

            final String str = res1.read(new Func<InputStream, String>() {
                @Override
                public String eval(InputStream in) {
                    return FileHelper.readInputStreamAsString(in, "UTF8");
                }
            });

            assertEquals(contentString, str);
            res1.getHadoopFileSystem().delete(res1.getHadoopPath(), false);

            assertFalse(res1.isExists());

        } finally {
            res1.close();
        }
    }
}

package org.apache.metamodel.util;

import java.io.InputStream;

import junit.framework.TestCase;

public class HdfsResourceTest extends TestCase {

    public void testGetQualifiedName() throws Exception {
        final HdfsResource res1 = new HdfsResource("hdfs://localhost:9000/home/metamodel.txt");
        assertEquals("hdfs://localhost:9000/home/metamodel.txt", res1.getQualifiedPath());
        assertEquals("metamodel.txt", res1.getName());

        final HdfsResource res2 = new HdfsResource("localhost", 9000, "/home/metamodel.txt");
        assertEquals("hdfs://localhost:9000/home/metamodel.txt", res2.getQualifiedPath());
        assertEquals("metamodel.txt", res2.getName());

        assertEquals(res1, res2);

        final HdfsResource res3 = new HdfsResource("localhost", 9000, "/home/apache.txt");
        assertEquals("hdfs://localhost:9000/home/apache.txt", res3.getQualifiedPath());
        assertEquals("apache.txt", res3.getName());

        assertFalse(res3.equals(res1));

        res1.close();
        res2.close();
        res3.close();
    }
}

package org.apache.metamodel.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class FileResourceTest {

    @Test
    public void testReadDirectory() throws Exception {
        final String contentString = "fun and games with Apache MetaModel and Hadoop is what we do";
        final String[] contents = new String[] { "fun ", "and ", "games ", "with ", "Apache ", "MetaModel ", "and ", "Hadoop ", "is ", "what ", "we ", "do" };

        Path path = Files.createTempDirectory("test");

        // Reverse both filename and contents to make sure it is the name and not the creation order that is sorted on.
        int i = contents.length;
        Collections.reverse(Arrays.asList(contents));
        for(final String contentPart : contents){
            final FileResource partResource = new FileResource(path + "/part-" + String.format("%02d", i--));
            partResource.write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream out) throws Exception {
                    out.write(contentPart.getBytes());
                }
            });
        }


        final FileResource res1 = new FileResource(path.toFile());

        final String str1 = res1.read(new Func<InputStream, String>() {
            @Override
            public String eval(InputStream in) {
                return FileHelper.readInputStreamAsString(in, "UTF8");
            }
        });

        Assert.assertEquals(contentString, str1);

        final String str2 = res1.read(new Func<InputStream, String>() {
            @Override
            public String eval(InputStream in) {
                return FileHelper.readInputStreamAsString(in, "UTF8");
            }
        });
        Assert.assertEquals(str1, str2);
    }

}
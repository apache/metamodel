package org.apache.metamodel.fixedwidth;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

public class FixedWidthReaderTest {

    @Test
    public void testBufferedReader() throws IOException {
        final File file = new File("src/test/resources/example_simple1.txt");
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        int[] widths = new int[] { 8, 9 };
        try (final FixedWidthReader fixedWidthReader = new FixedWidthReader(reader, widths, false)) {
            final String[] line1 = fixedWidthReader.readLine();
            assertEquals("[greeting, greeter]", Arrays.asList(line1).toString());
            final String[] line2 = fixedWidthReader.readLine();
            assertEquals("[hello, world]", Arrays.asList(line2).toString());
            final String[] line3 = fixedWidthReader.readLine();
            assertEquals("[hi, there]", Arrays.asList(line3).toString());
        }
    }

    @Test
    public void testNoBufferReader() throws IOException {
        int[] widths = new int[] { 8, 9 };
        final String lineToBeRead = "greeting  greeter  ";
        @SuppressWarnings("resource")
        final FixedWidthReader fixedWidthReader = new FixedWidthReader(null, widths, false);
        final String[] line = fixedWidthReader.readLine(lineToBeRead);
        assertEquals("[greeting, greeter]", Arrays.asList(line).toString());
    }
}

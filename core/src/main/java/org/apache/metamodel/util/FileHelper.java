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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various helper methods for handling files
 */
public final class FileHelper {

    private final static Logger logger = LoggerFactory.getLogger(FileHelper.class);

    public static final String UTF_8_ENCODING = "UTF-8";
    public static final String UTF_16_ENCODING = "UTF-16";
    public static final String US_ASCII_ENCODING = "US-ASCII";
    public static final String ISO_8859_1_ENCODING = "ISO_8859_1";
    public static final String DEFAULT_ENCODING = UTF_8_ENCODING;

    private FileHelper() {
        // prevent instantiation
    }

    public static File createTempFile(String prefix, String suffix) {
        try {
            return File.createTempFile(prefix, suffix);
        } catch (IOException e) {
            logger.error("Could not create tempFile", e);
            File tempDir = getTempDir();
            return new File(tempDir, prefix + '.' + suffix);
        }
    }

    public static File getTempDir() {
        File result = null;
        String tmpDirPath = System.getProperty("java.io.tmpdir");
        if (tmpDirPath != null && !"".equals(tmpDirPath)) {
            result = new File(tmpDirPath);
        } else {
            logger.debug("Could not determine tmpdir by using environment variable.");
            try {
                File file = File.createTempFile("foo", "bar");
                result = file.getParentFile();
                if (!file.delete()) {
                    logger.warn("Could not delete temp file '{}'", file.getAbsolutePath());
                }
            } catch (IOException e) {
                logger.error("Could not create tempFile in order to find temporary dir", e);
                result = new File("metamodel.tmp.dir");
                if (!result.mkdir()) {
                    throw new IllegalStateException("Could not create directory for temporary files: "
                            + result.getName());
                }
                result.deleteOnExit();
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info("Using '{}' as tmpdir.", result.getAbsolutePath());
        }
        return result;
    }

    public static Writer getWriter(File file, String encoding, boolean append) throws IllegalStateException {
        boolean insertBom = !append;
        return getWriter(file, encoding, append, insertBom);
    }

    public static Writer getWriter(OutputStream outputStream, String encoding) throws IllegalStateException {
        return getWriter(outputStream, encoding, false);
    }

    public static Writer getWriter(OutputStream outputStream, String encoding, boolean insertBom)
            throws IllegalStateException {
        if (!(outputStream instanceof BufferedOutputStream)) {
            outputStream = new BufferedOutputStream(outputStream);
        }

        try {
            if (insertBom) {
                Writer writer = new UnicodeWriter(outputStream, encoding);
                return writer;
            } else {
                Writer writer = new OutputStreamWriter(outputStream, encoding);
                return writer;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Writer getWriter(File file, String encoding, boolean append, boolean insertBom)
            throws IllegalStateException {
        if (append && insertBom) {
            throw new IllegalArgumentException("Can not insert BOM into appending writer");
        }
        final OutputStream outputStream = getOutputStream(file, append);
        return getWriter(outputStream, encoding, insertBom);

    }

    public static Writer getWriter(File file, String encoding) throws IllegalStateException {
        return getWriter(file, encoding, false);
    }

    public static Reader getReader(InputStream inputStream, String encoding) throws IllegalStateException {
        try {
            if (encoding == null || encoding.toLowerCase().indexOf("utf") != -1) {
                final byte bom[] = new byte[4];
                int unread;

                // auto-detect byte-order-mark
                @SuppressWarnings("resource")
                final PushbackInputStream pushbackInputStream = new PushbackInputStream(inputStream, bom.length);
                final int n = pushbackInputStream.read(bom, 0, bom.length);

                // Read ahead four bytes and check for BOM marks.
                if ((bom[0] == (byte) 0xEF) && (bom[1] == (byte) 0xBB) && (bom[2] == (byte) 0xBF)) {
                    encoding = "UTF-8";
                    unread = n - 3;
                } else if ((bom[0] == (byte) 0xFE) && (bom[1] == (byte) 0xFF)) {
                    encoding = "UTF-16BE";
                    unread = n - 2;
                } else if ((bom[0] == (byte) 0xFF) && (bom[1] == (byte) 0xFE)) {
                    encoding = "UTF-16LE";
                    unread = n - 2;
                } else if ((bom[0] == (byte) 0x00) && (bom[1] == (byte) 0x00) && (bom[2] == (byte) 0xFE)
                        && (bom[3] == (byte) 0xFF)) {
                    encoding = "UTF-32BE";
                    unread = n - 4;
                } else if ((bom[0] == (byte) 0xFF) && (bom[1] == (byte) 0xFE) && (bom[2] == (byte) 0x00)
                        && (bom[3] == (byte) 0x00)) {
                    encoding = "UTF-32LE";
                    unread = n - 4;
                } else {
                    unread = n;
                }

                if (unread > 0) {
                    pushbackInputStream.unread(bom, (n - unread), unread);
                } else if (unread < -1) {
                    pushbackInputStream.unread(bom, 0, 0);
                }

                inputStream = pushbackInputStream;
            }

            final InputStreamReader inputStreamReader;
            if (encoding == null) {
                inputStreamReader = new InputStreamReader(inputStream);
            } else {
                inputStreamReader = new InputStreamReader(inputStream, encoding);
            }
            return inputStreamReader;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Reader getReader(File file, String encoding) throws IllegalStateException {
        final InputStream inputStream;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(file));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return getReader(inputStream, encoding);
    }

    public static String readInputStreamAsString(InputStream inputStream, String encoding) throws IllegalStateException {
        Reader reader = getReader(inputStream, encoding);
        return readAsString(reader);
    }

    public static String readFileAsString(File file, String encoding) throws IllegalStateException {
        Reader br = getReader(file, encoding);
        return readAsString(br);
    }

    public static String readAsString(Reader reader) throws IllegalStateException {
        final BufferedReader br = getBufferedReader(reader);
        try {
            StringBuilder sb = new StringBuilder();
            boolean firstLine = true;
            for (String line = br.readLine(); line != null; line = br.readLine()) {
                if (firstLine) {
                    firstLine = false;
                } else {
                    sb.append('\n');
                }
                sb.append(line);
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            safeClose(br, reader);
        }
    }

    public static void safeClose(Object... objects) {
        boolean debugEnabled = logger.isDebugEnabled();

        if (objects == null || objects.length == 0) {
            logger.info("safeClose(...) was invoked with null or empty array: {}", objects);
            return;
        }

        for (Object obj : objects) {
            if (obj != null) {
                if (debugEnabled) {
                    logger.debug("Trying to safely close {}", obj);
                }

                if (obj instanceof Flushable) {
                    try {
                        ((Flushable) obj).flush();
                    } catch (Exception e) {
                        if (debugEnabled) {
                            logger.debug("Flushing Flushable failed", e);
                        }
                    }
                }

                if (obj instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) obj).close();
                    } catch (Exception e) {
                        if (debugEnabled) {
                            logger.debug("Closing AutoCloseable failed", e);
                        }
                    }
                } else {
                    logger.info("obj was not AutoCloseable, trying to find close() method via reflection.");

                    try {
                        Method method = obj.getClass().getMethod("close", new Class[0]);
                        if (method == null) {
                            logger.info("obj did not have a close() method, ignoring");
                        } else {
                            method.setAccessible(true);
                            method.invoke(obj);
                        }
                    } catch (InvocationTargetException e) {
                        logger.warn("Invoking close() by reflection threw exception", e);
                    } catch (Exception e) {
                        logger.warn("Could not invoke close() by reflection", e);
                    }
                }
            }

        }
    }

    public static BufferedWriter getBufferedWriter(File file, String encoding) throws IllegalStateException {
        Writer writer = getWriter(file, encoding);
        return new BufferedWriter(writer);
    }

    public static BufferedReader getBufferedReader(File file, String encoding) throws IllegalStateException {
        Reader reader = getReader(file, encoding);
        return new BufferedReader(reader);
    }

    public static BufferedReader getBufferedReader(InputStream inputStream, String encoding)
            throws IllegalStateException {
        Reader reader = getReader(inputStream, encoding);
        return new BufferedReader(reader);
    }

    public static Reader getReader(File file) throws IllegalStateException {
        return getReader(file, DEFAULT_ENCODING);
    }

    public static String readFileAsString(File file) throws IllegalStateException {
        return readFileAsString(file, DEFAULT_ENCODING);
    }

    public static BufferedWriter getBufferedWriter(File file) throws IllegalStateException {
        return getBufferedWriter(file, DEFAULT_ENCODING);
    }

    public static Writer getWriter(File file) throws IllegalStateException {
        return getWriter(file, DEFAULT_ENCODING);
    }

    public static void writeString(OutputStream outputStream, String string) throws IllegalStateException {
        writeString(outputStream, string, DEFAULT_ENCODING);
    }

    public static void writeString(OutputStream outputStream, String string, String encoding)
            throws IllegalStateException {
        final Writer writer = getWriter(outputStream, encoding);
        writeString(writer, string, encoding);
    }

    public static void writeString(Writer writer, String string) throws IllegalStateException {
        writeString(writer, string, DEFAULT_ENCODING);
    }

    public static void writeString(Writer writer, String string, String encoding) throws IllegalStateException {
        try {
            writer.write(string);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            safeClose(writer);
        }
    }

    public static void writeStringAsFile(File file, String string) throws IllegalStateException {
        writeStringAsFile(file, string, DEFAULT_ENCODING);
    }

    public static void writeStringAsFile(File file, String string, String encoding) throws IllegalStateException {
        final BufferedWriter bw = getBufferedWriter(file, encoding);
        writeString(bw, string, encoding);
    }

    public static BufferedReader getBufferedReader(File file) throws IllegalStateException {
        return getBufferedReader(file, DEFAULT_ENCODING);
    }

    public static void copy(Reader reader, Writer writer) throws IllegalStateException {
        final BufferedReader bufferedReader = getBufferedReader(reader);
        try {
            boolean firstLine = true;
            for (String line = bufferedReader.readLine(); line != null; line = bufferedReader.readLine()) {
                if (firstLine) {
                    firstLine = false;
                } else {
                    writer.write('\n');
                }
                writer.write(line);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static BufferedReader getBufferedReader(Reader reader) {
        if (reader instanceof BufferedReader) {
            return (BufferedReader) reader;
        }
        return new BufferedReader(reader);
    }

    public static void copy(InputStream fromStream, OutputStream toStream) throws IllegalStateException {
        try {
            byte[] buffer = new byte[1024 * 32];
            for (int read = fromStream.read(buffer); read != -1; read = fromStream.read(buffer)) {
                toStream.write(buffer, 0, read);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void copy(Resource from, Resource to) throws IllegalStateException {
        assert from.isExists();

        final InputStream in = from.read();
        try {
            final OutputStream out = to.write();
            try {
                copy(in, out);
            } finally {
                safeClose(out);
            }
        } finally {
            safeClose(in);
        }
    }

    public static void copy(File from, File to) throws IllegalStateException {
        assert from.exists();
        
        final InputStream in = getInputStream(from);
        try {
            final OutputStream out = getOutputStream(to);
            try {
                copy(in, out);
            } finally {
                safeClose(out);
            }
        } finally {
            safeClose(in);
        }
    }

    public static OutputStream getOutputStream(File file) throws IllegalStateException {
        return getOutputStream(file, false);
    }

    public static OutputStream getOutputStream(File file, boolean append) {
        try {
            return new BufferedOutputStream(new FileOutputStream(file, append));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public static InputStream getInputStream(File file) throws IllegalStateException {
        try {
            return new BufferedInputStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    public static byte[] readAsBytes(InputStream inputStream) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            copy(inputStream, baos);
        } finally {
            safeClose(inputStream);
        }
        return baos.toByteArray();
    }
}
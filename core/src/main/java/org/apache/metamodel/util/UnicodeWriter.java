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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

/**
 * Writes Unicode text to an output stream. If the specified encoding is a
 * Unicode, then the text is preceeded by the proper Unicode BOM. If it is any
 * other encoding, this class behaves just like <code>OutputStreamWriter</code>.
 * This class is here because Java's <code>OutputStreamWriter</code> apparently
 * doesn't believe in writing BOMs.
 * <p>
 * 
 * For optimum performance, it is recommended that you wrap all instances of
 * <code>UnicodeWriter</code> with a <code>java.io.BufferedWriter</code>.
 * 
 * This file is an adaption of Rubert Futrell from FifeSoft UnicodeWriter (BSD
 * licensed).
 * 
 * <pre>
 * UnicodeWriter.java - Writes Unicode output with the proper BOM.
 * Copyright (C) 2004 Robert Futrell
 * robert_futrell at users.sourceforge.net
 * http://fifesoft.com/rsyntaxtextarea
 * </pre>
 */
public class UnicodeWriter extends Writer {

    public static final byte[] UTF8_BOM = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };

    public static final byte[] UTF16LE_BOM = new byte[] { (byte) 0xFF, (byte) 0xFE };

    public static final byte[] UTF16BE_BOM = new byte[] { (byte) 0xFE, (byte) 0xFF };

    public static final byte[] UTF32LE_BOM = new byte[] { (byte) 0xFF, (byte) 0xFE, (byte) 0x00, (byte) 0x00 };

    public static final byte[] UTF32BE_BOM = new byte[] { (byte) 0x00, (byte) 0x00, (byte) 0xFE, (byte) 0xFF };

    /**
     * The writer actually doing the writing.
     */
    private final OutputStreamWriter writer;

    /**
     * This is a utility constructor since the vast majority of the time, this
     * class will be used to write Unicode files.
     * 
     * @param fileName
     *            The file to which to write the Unicode output.
     * @param encoding
     *            The encoding to use.
     * @throws UnsupportedEncodingException
     *             If the specified encoding is not supported.
     * @throws IOException
     *             If an IO exception occurs.
     */
    public UnicodeWriter(String fileName, String encoding) throws UnsupportedEncodingException, IOException {
        this(new FileOutputStream(fileName), encoding);
    }

    /**
     * This is a utility constructor since the vast majority of the time, this
     * class will be used to write Unicode files.
     * 
     * @param file
     *            The file to which to write the Unicode output.
     * @param encoding
     *            The encoding to use.
     * @throws UnsupportedEncodingException
     *             If the specified encoding is not supported.
     * @throws IOException
     *             If an IO exception occurs.
     */
    public UnicodeWriter(File file, String encoding) throws UnsupportedEncodingException, IOException {
        this(new FileOutputStream(file), encoding);
    }

    /**
     * Creates a new writer.
     * 
     * @param outputStream
     *            The output stream to write.
     * @param encoding
     *            The encoding to use.
     * @throws UnsupportedEncodingException
     *             If the specified encoding is not supported.
     * @throws IOException
     *             If an IO exception occurs.
     */
    public UnicodeWriter(OutputStream outputStream, String encoding) throws UnsupportedEncodingException, IOException {
        writer = createWriter(outputStream, encoding);
    }

    /**
     * Closes this writer.
     * 
     * @throws IOException
     *             If an IO exception occurs.
     */
    @Override
    public void close() throws IOException {
        writer.close();
    }

    /**
     * Flushes the stream.
     * 
     * @throws IOException
     *             If an IO exception occurs.
     */
    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    /**
     * Initializes the internal output stream and writes the BOM if the
     * specified encoding is a Unicode encoding.
     * 
     * @param outputStream
     *            The output stream we are writing.
     * @param encoding
     *            The encoding in which to write.
     * @throws UnsupportedEncodingException
     *             If the specified encoding isn't supported.
     * @throws IOException
     *             If an I/O error occurs while writing a BOM.
     */
    private OutputStreamWriter createWriter(OutputStream outputStream, String encoding)
            throws UnsupportedEncodingException, IOException {
        OutputStreamWriter writer = new OutputStreamWriter(outputStream, encoding);

        encoding = encoding.replaceAll("-", "").toUpperCase();

        // Write the proper BOM if they specified a Unicode encoding.
        // NOTE: Creating an OutputStreamWriter with encoding "UTF-16"
        // DOES write out the BOM; "UTF-16LE", "UTF-16BE", "UTF-32", "UTF-32LE"
        // and "UTF-32BE" don't.
        if ("UTF8".equals(encoding)) {
            outputStream.write(UTF8_BOM, 0, UTF8_BOM.length);
        } else if ("UTF16LE".equals(encoding)) {
            outputStream.write(UTF16LE_BOM, 0, UTF16LE_BOM.length);
        } else if ("UTF16BE".equals(encoding)) {
            outputStream.write(UTF16BE_BOM, 0, UTF16BE_BOM.length);
        } else if ("UTF32LE".equals(encoding)) {
            outputStream.write(UTF32LE_BOM, 0, UTF32LE_BOM.length);
        } else if ("UTF32".equals(encoding) || "UTF32BE".equals(encoding)) {
            outputStream.write(UTF32BE_BOM, 0, UTF32BE_BOM.length);
        }

        return writer;
    }

    /**
     * Writes a portion of an array of characters.
     * 
     * @param cbuf
     *            The buffer of characters.
     * @param off
     *            The offset from which to start writing characters.
     * @param len
     *            The number of characters to write.
     * @throws IOException
     *             If an I/O error occurs.
     */
    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        writer.write(cbuf, off, len);
    }

    /**
     * Writes a single character.
     * 
     * @param c
     *            An integer specifying the character to write.
     * @throws IOException
     *             If an IO error occurs.
     */
    @Override
    public void write(int c) throws IOException {
        writer.write(c);
    }

    /**
     * Writes a portion of a string.
     * 
     * @param str
     *            The string from which to write.
     * @param off
     *            The offset from which to start writing characters.
     * @param len
     *            The number of characters to write.
     * @throws IOException
     *             If an IO error occurs.
     */
    @Override
    public void write(String str, int off, int len) throws IOException {
        writer.write(str, off, len);
    }

}

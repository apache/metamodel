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
package org.apache.metamodel.csv;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.apache.metamodel.AbstractUpdateCallback;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.create.TableCreationBuilder;
import org.apache.metamodel.delete.RowDeletionBuilder;
import org.apache.metamodel.drop.TableDropBuilder;
import org.apache.metamodel.insert.RowInsertionBuilder;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.update.RowUpdationBuilder;
import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.EqualsBuilder;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CsvUpdateCallback extends AbstractUpdateCallback implements UpdateCallback {

    private static final Logger logger = LoggerFactory.getLogger(CsvUpdateCallback.class);

    private final CsvConfiguration _configuration;
    private final Resource _resource;
    private Writer _writer;

    public CsvUpdateCallback(CsvDataContext dataContext) {
        super(dataContext);
        _resource = dataContext.getResource();
        _configuration = dataContext.getConfiguration();
    }

    @Override
    public TableCreationBuilder createTable(Schema schema, String name) throws IllegalArgumentException,
            IllegalStateException {
        return new CsvCreateTableBuilder(this, schema, name);
    }

    @Override
    public RowInsertionBuilder insertInto(Table table) throws IllegalArgumentException, IllegalStateException {
        validateTable(table);
        return new CsvInsertBuilder(this, table);
    }

    public CsvConfiguration getConfiguration() {
        return _configuration;
    }

    public Resource getResource() {
        return _resource;
    }

    private void validateTable(Table table) {
        if (!(table instanceof CsvTable)) {
            throw new IllegalArgumentException("Not a valid CSV table: " + table);
        }
    }

    protected synchronized void writeRow(final String[] stringValues, final boolean append) {
        final CsvWriter csvWriter = new CsvWriter(_configuration);
        final String line = csvWriter.buildLine(stringValues);
        final Writer writer = getWriter(append);
        try {
            writer.write(line);
        } catch (IOException e) {
            throw new MetaModelException("Failed to write line: " + line, e);
        }
    }

    private Writer getWriter(boolean append) {
        if (_writer == null || !append) {
            final boolean needsLineBreak = needsLineBreak(_resource, _configuration);

            final OutputStream out;
            if (append) {
                out = _resource.append();
            } else {
                out = _resource.write();
            }

            final boolean insertBom = !append;

            final Writer writer = FileHelper.getWriter(out, _configuration.getEncoding(), insertBom);

            if (needsLineBreak) {
                try {
                    writer.write('\n');
                } catch (IOException e) {
                    logger.debug("Failed to insert newline", e);
                }
            }
            _writer = writer;
        }
        return _writer;
    }

    protected static boolean needsLineBreak(Resource resource, CsvConfiguration configuration) {
        if (!resource.isExists() || resource.getSize() == 0) {
            return false;
        }

        if (resource instanceof FileResource) {

            final File file = ((FileResource) resource).getFile();

            try {
                // find the bytes a newline would match under the encoding
                final byte[] bytesInLineBreak;
                {
                    ByteBuffer encodedLineBreak = Charset.forName(configuration.getEncoding()).encode("\n");
                    bytesInLineBreak = new byte[encodedLineBreak.capacity()];
                    encodedLineBreak.get(bytesInLineBreak);
                }

                // find the last bytes of the file
                final byte[] bytesFromFile = new byte[bytesInLineBreak.length];
                {
                    final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
                    try {
                        FileChannel channel = randomAccessFile.getChannel();
                        try {
                            long length = randomAccessFile.length();

                            channel = channel.position(length - bytesInLineBreak.length);
                            channel.read(ByteBuffer.wrap(bytesFromFile));
                        } finally {
                            channel.close();
                        }
                    } finally {
                        randomAccessFile.close();
                    }
                }

                // if the two byte arrays match, then the newline is not needed.
                if (EqualsBuilder.equals(bytesInLineBreak, bytesFromFile)) {
                    return false;
                }
                return true;
            } catch (Exception e) {
                logger.error("Error occurred while checking if file needs linebreak, omitting check", e);
            }
        }

        return false;
    }

    /**
     * Closes all open handles
     */
    protected void close() {
        if (_writer != null) {
            try {
                _writer.flush();
            } catch (IOException e) {
                logger.warn("Failed to flush CSV writer", e);
            }
            try {
                _writer.close();
            } catch (IOException e) {
                logger.error("Failed to close CSV writer", e);
            } finally {
                _writer = null;
            }
        }
    }

    @Override
    public RowUpdationBuilder update(Table table) throws IllegalArgumentException, IllegalStateException {
        close();
        return super.update(table);
    }

    @Override
    public boolean isDropTableSupported() {
        return true;
    }

    @Override
    public TableDropBuilder dropTable(Table table) {
        validateTable(table);
        return new CsvTableDropBuilder(this, table);
    }

    /**
     * Callback method used by {@link CsvTableDropBuilder} when execute is
     * called
     */
    protected void dropTable() {
        close();
        if (_resource instanceof FileResource) {
            final File file = ((FileResource) _resource).getFile();
            final boolean success = file.delete();
            if (!success) {
                throw new MetaModelException("Could not delete (drop) file: " + file);
            }
        } else {
            _resource.write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream arg) throws Exception {
                    // do nothing, just write an empty file
                }
            });
        }
    }

    @Override
    public boolean isDeleteSupported() {
        return true;
    }

    @Override
    public RowDeletionBuilder deleteFrom(Table table) {
        validateTable(table);
        return new CsvDeleteBuilder(this, table);
    }
}

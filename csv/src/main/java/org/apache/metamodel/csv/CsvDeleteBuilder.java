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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.UpdateCallback;
import org.apache.metamodel.UpdateScript;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.delete.AbstractRowDeletionBuilder;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CsvDeleteBuilder extends AbstractRowDeletionBuilder {

    private static final Logger logger = LoggerFactory.getLogger(CsvDeleteBuilder.class);

    private final CsvUpdateCallback _updateCallback;

    public CsvDeleteBuilder(CsvUpdateCallback updateCallback, Table table) {
        super(table);
        _updateCallback = updateCallback;
    }

    @Override
    public void execute() throws MetaModelException {
        final File tempFile = FileHelper.createTempFile("metamodel_deletion", "csv");

        final CsvConfiguration configuration = _updateCallback.getConfiguration();

        final CsvDataContext copyDataContext = new CsvDataContext(tempFile, configuration);
        copyDataContext.executeUpdate(new UpdateScript() {

            @Override
            public void run(UpdateCallback callback) {
                final Table originalTable = getTable();
                final Table copyTable = callback.createTable(copyDataContext.getDefaultSchema(), originalTable.getName())
                        .like(originalTable).execute();

                if (isTruncateTableOperation()) {
                    // no need to iterate old records, they should all be
                    // removed
                    return;
                }

                final DataSet dataSet = _updateCallback.getDataContext().query().from(originalTable)
                        .select(originalTable.getColumns()).execute();
                try {
                    while (dataSet.next()) {
                        final Row row = dataSet.getRow();
                        if (!deleteRow(row)) {
                            callback.insertInto(copyTable).like(row).execute();
                        }
                    }
                } finally {
                    dataSet.close();
                }
            }
        });

        // copy the copy (which does not have deleted records) to overwrite the
        // original
        final InputStream in = FileHelper.getInputStream(tempFile);
        try {
            _updateCallback.getResource().write(new Action<OutputStream>() {
                @Override
                public void run(OutputStream out) throws Exception {
                    FileHelper.copy(in, out);
                }
            });
        } finally {
            FileHelper.safeClose(in);
        }

        final boolean deleted = tempFile.delete();
        if (!deleted) {
            logger.warn("Could not delete temporary copy-file: {}", tempFile);
        }
    }
}

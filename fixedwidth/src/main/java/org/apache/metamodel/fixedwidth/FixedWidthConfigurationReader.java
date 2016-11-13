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
package org.apache.metamodel.fixedwidth;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.metamodel.csv.CsvConfiguration;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.Table;
import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object capable of reading fixed width metadata from external sources and
 * thereby producing an appropriate {@link FixedWidthConfiguration} to use with
 * a {@link FixedWidthDataContext}.
 */
public class FixedWidthConfigurationReader {

    private static final Logger logger = LoggerFactory.getLogger(FixedWidthConfigurationReader.class);

    // example: @1 COL1 $char1.
    private final Pattern PATTERN_SAS_INPUT_LINE = Pattern.compile("\\@(\\d+) (.+) .*?(\\d+)\\.");

    // example: COL1 "Record type"
    private final Pattern PATTERN_SAS_LABEL_LINE = Pattern.compile("(.+) \\\"(.+)\\\"");

    /**
     * Reads a {@link FixedWidthConfiguration} based on a SAS 'format file',
     * <a href=
     * "http://support.sas.com/documentation/cdl/en/etlug/67323/HTML/default/viewer.htm#p0h03yig7fp1qan1arghp3lwjqi6.htm">
     * described here</a>.
     * 
     * @param encoding the format file encoding
     * @param resource the format file resource 
     * @param failOnInconsistentLineWidth flag specifying whether inconsistent line should stop processing or not
     * @return a {@link FixedWidthConfiguration} object to use
     */
    public FixedWidthConfiguration readFromSasFormatFile(String encoding, Resource resource,
            boolean failOnInconsistentLineWidth) {
        final List<FixedWidthColumnSpec> columnSpecs = new ArrayList<>();

        final CsvDataContext dataContext = new CsvDataContext(resource, new CsvConfiguration());
        final Table table = dataContext.getDefaultSchema().getTable(0);
        try (final DataSet dataSet = dataContext.query().from(table).select("Name", "BeginPosition", "EndPosition")
                .execute()) {
            while (dataSet.next()) {
                final String name = (String) dataSet.getRow().getValue(0);
                final int beginPosition = Integer.parseInt((String) dataSet.getRow().getValue(1));
                final int endPosition = Integer.parseInt((String) dataSet.getRow().getValue(2));
                final int width = 1 + endPosition - beginPosition;
                columnSpecs.add(new FixedWidthColumnSpec(name, width));
            }
        }

        return new FixedWidthConfiguration(encoding, columnSpecs, failOnInconsistentLineWidth);
    }

    /**
     * Reads a {@link FixedWidthConfiguration} based on a SAS INPUT declaration.
     * The reader method also optionally will look for a LABEL definition for column naming.
     * 
     * @param encoding the format file encoding
     * @param resource the format file resource
     * @param failOnInconsistentLineWidth flag specifying whether inconsistent line should stop processing or not
     * @return a {@link FixedWidthConfiguration} object to use
     */
    public FixedWidthConfiguration readFromSasInputDefinition(String encoding, Resource resource,
            boolean failOnInconsistentLineWidth) {

        final Map<String, Integer> inputWidthDeclarations = new LinkedHashMap<>();
        final Map<String, String> labelDeclarations = new HashMap<>();

        resource.read(new Action<InputStream>() {

            private boolean inInputSection = false;
            private boolean inLabelSection = false;

            @Override
            public void run(InputStream in) throws Exception {
                try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                        processLine(line);
                    }
                }
            }

            private void processLine(String line) {
                line = line.trim();
                if (line.isEmpty()) {
                    return;
                }
                if (";".equals(line)) {
                    inInputSection = false;
                    inLabelSection = false;
                    return;
                } else if ("INPUT".equals(line)) {
                    inInputSection = true;
                    return;
                } else if ("LABEL".equals(line)) {
                    inLabelSection = true;
                    return;
                }

                if (inInputSection) {
                    final Matcher matcher = PATTERN_SAS_INPUT_LINE.matcher(line);
                    if (matcher.matches()) {
                        final String positionSpec = matcher.group(1);
                        final String nameSpec = matcher.group(2);
                        final int width = Integer.parseInt(matcher.group(3));
                        logger.debug("Parsed INPUT line \"{}\": position={}, name={}, width={}", line, positionSpec,
                                nameSpec, width);
                        inputWidthDeclarations.put(nameSpec, width);
                    } else {
                        logger.debug("Failed to parse/recognize INPUT line \"{}\"", line);
                    }
                } else if (inLabelSection) {
                    final Matcher matcher = PATTERN_SAS_LABEL_LINE.matcher(line);
                    if (matcher.matches()) {
                        final String nameSpec = matcher.group(1);
                        final String labelSpec = matcher.group(2);
                        logger.debug("Parsed LABEL line \"{}\": name={}, label={}", line, nameSpec, labelSpec);
                        labelDeclarations.put(nameSpec, labelSpec);
                    } else {
                        logger.debug("Failed to parse/recognize LABEL line \"{}\"", line);
                    }
                }

                if (line.endsWith(";")) {
                    inInputSection = false;
                    inLabelSection = false;
                }
            }
        });

        final List<FixedWidthColumnSpec> columnSpecs = new ArrayList<>();
        for (Entry<String, Integer> entry : inputWidthDeclarations.entrySet()) {
            final String columnKey = entry.getKey();
            final Integer columnWidth = entry.getValue();
            final String columnLabel = labelDeclarations.get(columnKey);
            final String columnName = columnLabel == null ? columnKey : columnLabel;
            columnSpecs.add(new FixedWidthColumnSpec(columnName, columnWidth));
        }

        return new FixedWidthConfiguration(encoding, columnSpecs, failOnInconsistentLineWidth);
    }
}

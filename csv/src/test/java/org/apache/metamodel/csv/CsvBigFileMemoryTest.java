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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.Query;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Table;

public class CsvBigFileMemoryTest extends TestCase {

    private final int hugeFileRows = 3000;
    private final int hugeFileCols = 2000;

    private File getHugeFile() {
        final File file = new File("target/huge_csv.csv");
        if (!file.exists()) {

            final ExampleDataGenerator exampleDataGenerator = new ExampleDataGenerator(hugeFileRows, hugeFileCols);
            exampleDataGenerator.createFile(file);
        }
        return file;
    }

    /**
     * Runs a performance test based on the data created by the
     * ExampleDataCreator utility.
     * 
     * @see ExampleDataGenerator
     * @throws Exception
     */
    public void testHugeFile() throws Exception {
        final File file = getHugeFile();

        final long timeAtStart = System.currentTimeMillis();
        System.out.println("time at start: " + timeAtStart);

        final DataContext dc = new CsvDataContext(file, new CsvConfiguration(1, false, false));
        final Table t = dc.getDefaultSchema().getTables().get(0);

        final long timeAfterDataContext = System.currentTimeMillis();
        System.out.println("time after DataContext: " + timeAfterDataContext);

        final Query q = new Query().select(t.getColumns()).from(t);
        DataSet ds = dc.executeQuery(q);

        long timeAfterQuery = System.currentTimeMillis();
        System.out.println("time after query: " + timeAfterQuery);

        final CountDownLatch countDown = new CountDownLatch(hugeFileRows);
        final AtomicBoolean success = new AtomicBoolean(true);

        ExecutorService executorService = Executors.newFixedThreadPool(30);

        while (ds.next()) {
            final Row row = ds.getRow();
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    if (hugeFileCols != row.getValues().length) {
                        System.out.println("Weird row: " + row);
                        success.set(false);
                    }
                    countDown.countDown();
                }
            });
        }
        ds.close();

        countDown.await();
        assertTrue(success.get());

        executorService.shutdown();

        long timeAfterDataSet = System.currentTimeMillis();
        System.out.println("time after dataSet: " + timeAfterDataSet);

        long totalTime = timeAfterDataSet - timeAfterDataContext;
        System.out.println("Total time to process large file: " + totalTime + " millis");

        // results with old impl: [13908, 13827, 14577]. Total= 42312
        
        // results with new impl: [9052, 9200, 8193]. Total= 26445

        if (!file.delete()) {
            file.deleteOnExit();
        }
    }

    public void testApproximatedCountHugeFile() throws Exception {
        DataContext dc = new CsvDataContext(getHugeFile());

        Table table = dc.getDefaultSchema().getTables().get(0);
        Query q = dc.query().from(table).selectCount().toQuery();
        SelectItem selectItem = q.getSelectClause().getItem(0);
        selectItem.setFunctionApproximationAllowed(true);

        DataSet ds = dc.executeQuery(q);
        assertTrue(ds.next());
        Object[] values = ds.getRow().getValues();
        assertEquals(1, values.length);
        assertEquals(3332, ((Long) ds.getRow().getValue(selectItem)).intValue());
        assertEquals(3332, ((Long) values[0]).intValue());
        assertFalse(ds.next());
    }
}

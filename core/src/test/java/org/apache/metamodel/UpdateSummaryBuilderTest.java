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
package org.apache.metamodel;

import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class UpdateSummaryBuilderTest extends TestCase {

    public void testEmptyBuilder() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddInsert() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addInsert();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(1), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddInserts() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addInserts(10);
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(10), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testMakeInsertsUnknown() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addInserts(10);
        usb.makeInsertsUnknown();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.empty(), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddUpdate() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addUpdate();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(1), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddUpdates() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addUpdates(10);
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(10), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testMakeUpdatesUnknown() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addUpdates(10);
        usb.makeUpdatesUnknown();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.empty(), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddDelete() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addDelete();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(1), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddDeletes() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addDeletes(10);
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(10), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testMakeDeletesUnknown() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        usb.addDeletes(10);
        usb.makeDeletesUnknown();
        UpdateSummary us = usb.build();
        assertEquals(Optional.empty(), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    public void testAddGeneratedKey() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        Object key = new Object();
        usb.addGeneratedKey(key);
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertTrue(us.getGeneratedKeys().isPresent());
        assertEquals(1, getCount(us.getGeneratedKeys()));
    }

    public void testAddGeneratedKeys() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        Object key = new Object();
        Object key2 = new Object();
        List<Object> keyList = new ArrayList<>();
        keyList.add(key);
        keyList.add(key2);
        usb.addGeneratedKeys(keyList);
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertTrue(us.getGeneratedKeys().isPresent());
        assertEquals(2, getCount(us.getGeneratedKeys()));
    }

    public void testMakeGeneratedKeysUnknown() {
        UpdateSummaryBuilder usb = new UpdateSummaryBuilder();
        Object key = new Object();
        usb.addGeneratedKey(key);
        usb.makeGeneratedKeysUnknown();
        UpdateSummary us = usb.build();
        assertEquals(Optional.of(0), us.getDeletedRows());
        assertEquals(Optional.of(0), us.getInsertedRows());
        assertEquals(Optional.of(0), us.getUpdatedRows());
        assertEquals(Optional.empty(), us.getGeneratedKeys());
    }

    private int getCount(Optional<Iterable<Object>> objects) {
        int count = 0;
        Iterator<Object> it = objects.get().iterator();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }
}

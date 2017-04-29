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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;

/**
 * Abstract implementation of many methods in {@link Resource}
 */
public abstract class AbstractResource implements Resource {
    

    @Override
    public final void read(Action<InputStream> readCallback) {
        final InputStream in = read();
        try {
            readCallback.run(in);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }
    
    @Override
    public <E> E read(Function<InputStream, E> readCallback) throws ResourceException {
        final InputStream in = read();
        try {
            final E result = readCallback.apply(in);
            return result;
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }

    @Override
    public final <E> E read(UncheckedFunc<InputStream, E> readCallback) {
        final InputStream in = read();
        try {
            final E result = readCallback.applyUnchecked(in);
            return result;
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }

    @Override
    public final void write(Action<OutputStream> writeCallback) throws ResourceException {
        final OutputStream out = write();
        try {
            writeCallback.run(out);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in write callback", e);
        } finally {
            FileHelper.safeClose(out);
        }
    }

    @Override
    public final void append(Action<OutputStream> appendCallback) throws ResourceException {
        final OutputStream out = append();
        try {
            appendCallback.run(out);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in append callback", e);
        } finally {
            FileHelper.safeClose(out);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getQualifiedPath() + "]";
    }
}

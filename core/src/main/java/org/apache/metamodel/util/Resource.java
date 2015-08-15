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

/**
 * Represents a resource from which we can read and write bytes
 */
public interface Resource extends HasName {

    /**
     * Gets the name of the resource, typically a filename or other identifying
     * string
     */
    @Override
    public String getName();

    /**
     * Gets the qualified path of the resource, which typically includes slash
     * or backslash separated nodes in a hierarical tree structure.
     *
     * @return
     */
    public String getQualifiedPath();

    /**
     * Determines if the file is read only, or if writes are also possible.
     * 
     * @return
     */
    public boolean isReadOnly();

    /**
     * Determines if the resource referenced by this object exists or not.
     * 
     * @return
     */
    public boolean isExists();

    /**
     * Gets the size (in number of bytes) of this resource's data. An
     * approximated number is allowed.
     * 
     * If the size is not determinable without actually reading through the
     * whole contents of the resource, -1 is returned.
     * 
     * @return
     */
    public long getSize();

    /**
     * Gets the last modified timestamp value (measured in milliseconds since
     * the epoch (00:00:00 GMT, January 1, 1970)) of the resource, if available.
     * If the last modified date is not available, -1 is returned.
     * 
     * @return
     */
    public long getLastModified();

    /**
     * Opens up an {@link OutputStream} to write to the resource, and allows a
     * callback to perform writing actions on it.
     * 
     * @param writeCallback
     *            a callback which should define what to write to the resource.
     * 
     * @throws ResourceException
     *             if an error occurs while writing
     */
    public void write(Action<OutputStream> writeCallback) throws ResourceException;

    /**
     * Opens up an {@link OutputStream} to write to the resource. Consumers of
     * this method are expected to invoke the {@link OutputStream#close()}
     * method manually.
     * 
     * If possible, the other write(...) method is preferred over this one,
     * since it guarantees proper closing of the resource's handles.
     * 
     * @return
     * @throws ResourceException
     */
    public OutputStream write() throws ResourceException;

    /**
     * Opens up an {@link InputStream} to append (write at the end of the
     * existing stream) to the resource.
     * 
     * @param appendCallback
     *            a callback which should define what to append to the resource.
     * @throws ResourceException
     *             if an error occurs while appending
     */
    public void append(Action<OutputStream> appendCallback) throws ResourceException;

    /**
     * Opens up an {@link OutputStream} to append to the resource. Consumers of
     * this method are expected to invoke the {@link OutputStream#close()}
     * method manually.
     * 
     * If possible, the other append(...) method is preferred over this one,
     * since it guarantees proper closing of the resource's handles.
     * 
     * @return
     * @throws ResourceException
     */
    public OutputStream append() throws ResourceException;

    /**
     * Opens up an {@link InputStream} to read from the resource. Consumers of
     * this method are expected to invoke the {@link InputStream#close()} method
     * manually.
     * 
     * If possible, the other read(...) methods are preferred over this one,
     * since they guarantee proper closing of the resource's handles.
     * 
     * @return
     * @throws ResourceException
     */
    public InputStream read() throws ResourceException;

    /**
     * Opens up an {@link InputStream} to read from the resource, and allows a
     * callback to perform writing actions on it.
     * 
     * @param readCallback
     * 
     * @throws ResourceException
     *             if an error occurs while reading
     */
    public void read(Action<InputStream> readCallback) throws ResourceException;

    /**
     * Opens up an {@link InputStream} to read from the resource, and allows a
     * callback function to perform writing actions on it and return the
     * function's result.
     * 
     * @param readCallback
     * @return the result of the function
     * 
     * @throws ResourceException
     *             if an error occurs while reading
     */
    public <E> E read(Func<InputStream, E> readCallback) throws ResourceException;
}

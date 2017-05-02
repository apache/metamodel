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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a lazy loaded reference.
 * 
 * @param <E>
 */
public abstract class LazyRef<E> implements Supplier<E> {

    private static final Logger logger = LoggerFactory.getLogger(LazyRef.class);

    private final AtomicBoolean _fetched = new AtomicBoolean(false);
    private volatile Throwable _error;
    private E _object;

    @Override
    public final E get() {
        if (!_fetched.get()) {
            synchronized (_fetched) {
                if (!_fetched.get()) {
                    try {
                        _object = fetch();
                    } catch (Throwable t) {
                        _error = t;
                        if (t instanceof RuntimeException) {
                            throw (RuntimeException) t;
                        }
                        logger.warn("Failed to fetch value: " + this + ". Reporting error.", t);
                    } finally {
                        _fetched.set(true);
                    }
                }
            }
        }
        return _object;
    }

    protected abstract E fetch() throws Throwable;

    /**
     * Gets whether the lazy loaded reference has been loaded or not.
     * 
     * @return a boolean indicating whether or not the reference has been loaded
     *         or not
     */
    public boolean isFetched() {
        return _fetched.get();
    }

    /**
     * Gets any error that occurred while fetching the lazy loaded value.
     * 
     * @return
     */
    public Throwable getError() {
        return _error;
    }

    /**
     * Requests an asynchronous load of the lazy reference. If not already
     * loaded, this will cause another thread to load the reference, typically
     * to make it immediately available for later evaluation.
     * 
     * @param errorAction
     *            an optional error action to invoke if an exception is thrown
     *            during loading of the reference.
     */
    public void requestLoad(final Action<Throwable> errorAction) {
        if (!isFetched()) {
            ExecutorService executorService = SharedExecutorService.get();
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        get();
                    } catch (RuntimeException e) {
                        // do nothing, the exception will be handled via _error
                        // below
                    }
                    if (_error != null && errorAction != null) {
                        try {
                            errorAction.run(_error);
                        } catch (Exception e) {
                            logger.error("Error handling action failed!", e);
                        }
                    }
                }
            });
        }
    }

    /**
     * Requests an asynchronous load of the lazy reference. If not already
     * loaded, this will cause another thread to load the reference, typically
     * to make it immediately available for later evaluation.
     */
    public void requestLoad() {
        requestLoad(null);
    }

}

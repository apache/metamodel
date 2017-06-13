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
package org.apache.metamodel.service.app;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.factory.DataContextProperties;
import org.apache.metamodel.service.app.exceptions.DataSourceAlreadyExistException;
import org.apache.metamodel.service.app.exceptions.NoSuchDataSourceException;
import org.apache.metamodel.util.FileHelper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * A wrapper that adds a cache around a {@link DataSourceRegistry} in order to
 * prevent re-connecting all the time to the same data source.
 */
public class CachedDataSourceRegistryWrapper implements DataSourceRegistry {

    /**
     * The default timeout (in seconds) before the cache evicts and closes the
     * created {@link DataContext}s.
     */
    public static final int DEFAULT_TIMEOUT_SECONDS = 60;

    private final DataSourceRegistry delegate;
    private final LoadingCache<String, DataContext> loadingCache;

    public CachedDataSourceRegistryWrapper(DataSourceRegistry delegate) {
        this(delegate, DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public CachedDataSourceRegistryWrapper(DataSourceRegistry delegate, long cacheTimeout, TimeUnit cacheTimeoutUnit) {
        this.delegate = delegate;
        this.loadingCache = CacheBuilder.newBuilder().expireAfterAccess(cacheTimeout, cacheTimeoutUnit).removalListener(
                createRemovalListener()).build(createCacheLoader());
    }

    private RemovalListener<String, DataContext> createRemovalListener() {
        return new RemovalListener<String, DataContext>() {
            @Override
            public void onRemoval(RemovalNotification<String, DataContext> notification) {
                final DataContext dataContext = notification.getValue();
                // some DataContexts are closeable - attempt closing it here
                FileHelper.safeClose(dataContext);
            }
        };
    }

    private CacheLoader<String, DataContext> createCacheLoader() {
        return new CacheLoader<String, DataContext>() {
            @Override
            public DataContext load(String key) throws Exception {
                return delegate.openDataContext(key);
            }
        };
    }

    @Override
    public List<String> getDataSourceNames() {
        return delegate.getDataSourceNames();
    }

    @Override
    public String registerDataSource(String dataContextName, DataContextProperties dataContextProperties)
            throws DataSourceAlreadyExistException {
        loadingCache.invalidate(dataContextName);
        return delegate.registerDataSource(dataContextName, dataContextProperties);
    }

    @Override
    public DataContext openDataContext(String dataSourceName) throws NoSuchDataSourceException {
        try {
            return loadingCache.get(dataSourceName);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new MetaModelException("Unexpected error happened while getting DataContext '" + dataSourceName
                    + "' from cache", e);
        }
    }

}

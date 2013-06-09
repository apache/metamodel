/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a lazy loaded reference.
 * 
 * @author Kasper Sørensen
 * 
 * @param <E>
 */
public abstract class LazyRef<E> implements Ref<E> {

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

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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shared {@link ExecutorService} for use on ad-hoc tasks that can be
 * optimized by running operations in parallel.
 * 
 * Note that since this {@link ExecutorService} is shared, it is not recommended
 * to use it for dedicated tasks or daemon-like long-running tasks.
 * 
 * @author Kasper Sørensen
 */
public final class SharedExecutorService {

	/**
	 * A custom thread factory that creates daemon threads.
	 */
	private static final class ThreadFactoryImpl implements ThreadFactory {

		private static final AtomicInteger counter = new AtomicInteger(0);

		private final ThreadGroup _threadGroup;

		public ThreadFactoryImpl() {
			SecurityManager s = System.getSecurityManager();
			_threadGroup = (s != null) ? s.getThreadGroup() : Thread
					.currentThread().getThreadGroup();
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread thread = new Thread(_threadGroup, r,
					"MetaModel.SharedExecutorService.Thread."
							+ counter.incrementAndGet());
			thread.setDaemon(true);
			thread.setPriority(Thread.NORM_PRIORITY);
			return thread;
		}
	}

	private static final ExecutorService executor = Executors
			.newCachedThreadPool(new ThreadFactoryImpl());

	private SharedExecutorService() {
		// prevent instantiation
	}

	/**
	 * Gets the shared {@link ExecutorService}.
	 * 
	 * @return an {@link ExecutorService} for shared usage.
	 */
	public static final ExecutorService get() {
		return executor;
	}
}

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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A shared {@link ExecutorService} for use on ad-hoc tasks that can be
 * optimized by running operations in parallel.
 * 
 * Note that since this {@link ExecutorService} is shared, it is not recommended
 * to use it for dedicated tasks or daemon-like long-running tasks.
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

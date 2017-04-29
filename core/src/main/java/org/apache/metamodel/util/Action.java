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

import java.util.function.Consumer;

/**
 * Represents an abstract action, which is an executable piece of functionality
 * that takes an argument. An action is very similar to a {@link Consumer},
 * except that it allows for throwing exceptions, making it more appropriate for
 * encapsulating code blocks that may fail.
 * 
 * @param <E>
 *            the argument type of the action
 */
@FunctionalInterface
public interface Action<E> extends Consumer<E> {

    @Override
    default void accept(E t) {
        // delegate to run method and propagate exceptions if needed
        try {
            run(t);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    public void run(E arg) throws Exception;
}

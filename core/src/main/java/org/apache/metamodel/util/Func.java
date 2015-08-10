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

/**
 * Represents an abstract function, which is an executable piece of
 * functionality that has an input and an output. A {@link Func} has a return
 * type, unlike an {@link Action}.
 * 
 * @param <I>
 *            the input type
 * @param <O>
 *            the output type
 */
public interface Func<I, O> {

    /**
     * Evaluates an element and transforms it using this function.
     * 
     * @param arg
     *            the input given to the function
     * @return the output result of the function
     */
    public O eval(I arg);
}

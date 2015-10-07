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
package org.apache.metamodel.data;

/**
 * Sub-interface for {@link DataSet}s that wrap other {@link DataSet}s,
 * typically to apply some client-side filtering or enhancement logic on raw
 * data.
 */
public interface WrappingDataSet extends DataSet {

    /**
     * Gets the {@link DataSet} that is wrapped by this {@link WrappingDataSet}.
     * 
     * @return
     */
    public DataSet getWrappedDataSet();
}

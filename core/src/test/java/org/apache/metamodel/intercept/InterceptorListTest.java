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
package org.apache.metamodel.intercept;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.MockUpdateableDataContext;
import org.apache.metamodel.convert.ConvertedDataSetInterceptor;
import org.apache.metamodel.data.DataSet;

import junit.framework.TestCase;

public class InterceptorListTest extends TestCase {

	public void testGetInterceptorOfType() throws Exception {
		DataContext dc = new MockUpdateableDataContext();
		InterceptableDataContext interceptor = Interceptors.intercept(dc);
		
		InterceptorList<DataSet> list = interceptor.getDataSetInterceptors();
		ConvertedDataSetInterceptor convertedDataSetInterceptor = new ConvertedDataSetInterceptor();
		list.add(convertedDataSetInterceptor);
		
		assertSame(convertedDataSetInterceptor, list.getInterceptorOfType(DataSetInterceptor.class));
		assertSame(convertedDataSetInterceptor, list.getInterceptorOfType(ConvertedDataSetInterceptor.class));
		
		class NoopDataSetInterceptor implements DataSetInterceptor {
			@Override
			public DataSet intercept(DataSet dataSet) {
				return dataSet;
			}
		};
		
		NoopDataSetInterceptor noopDataSetInterceptor = new NoopDataSetInterceptor();
		list.add(noopDataSetInterceptor);
		
		assertSame(convertedDataSetInterceptor, list.getInterceptorOfType(DataSetInterceptor.class));
		assertSame(convertedDataSetInterceptor, list.getInterceptorOfType(ConvertedDataSetInterceptor.class));
		assertSame(noopDataSetInterceptor, list.getInterceptorOfType(NoopDataSetInterceptor.class));
		
		list.remove(convertedDataSetInterceptor);
		
		assertSame(noopDataSetInterceptor, list.getInterceptorOfType(DataSetInterceptor.class));
		assertNull(list.getInterceptorOfType(ConvertedDataSetInterceptor.class));
		assertSame(noopDataSetInterceptor, list.getInterceptorOfType(NoopDataSetInterceptor.class));
	}
}

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
package org.eobjects.metamodel.intercept;

import org.eobjects.metamodel.DataContext;
import org.eobjects.metamodel.MockUpdateableDataContext;
import org.eobjects.metamodel.convert.ConvertedDataSetInterceptor;
import org.eobjects.metamodel.data.DataSet;

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

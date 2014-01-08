package org.apache.metamodel.spring;

import static org.junit.Assert.assertEquals;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.pojo.PojoDataContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:examples/mock-datacontext-type.xml")
public class MockDataContextTypeTest {

    @Autowired
    private DataContext dataContext;

    @Test
    public void testReadDataContext() {
        assertEquals(PojoDataContext.class, dataContext.getClass());

        assertEquals("here's a value from springs XML file", dataContext.getDefaultSchema().getName());
    }
}

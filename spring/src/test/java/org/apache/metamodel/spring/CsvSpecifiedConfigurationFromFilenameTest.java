package org.apache.metamodel.spring;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.csv.CsvDataContext;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:examples/csv-using-filename.xml")
public class CsvSpecifiedConfigurationFromFilenameTest {

    @Autowired
    private DataContext dataContext;

    @Test
    public void testReadDataContext() {
        assertEquals(CsvDataContext.class, dataContext.getClass());

        CsvDataContext csv = (CsvDataContext) dataContext;
        Resource resource = csv.getResource();

        assertEquals(FileResource.class, resource.getClass());

        assertEquals("example.csv", resource.getName());

        assertEquals("[\"foo\", bar]", Arrays.toString(csv.getDefaultSchema().getTable(0).getColumnNames()));
    }
}

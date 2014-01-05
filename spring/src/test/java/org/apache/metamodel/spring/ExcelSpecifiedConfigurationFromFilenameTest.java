package org.apache.metamodel.spring;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.metamodel.DataContext;
import org.apache.metamodel.excel.ExcelDataContext;
import org.apache.metamodel.util.FileResource;
import org.apache.metamodel.util.Resource;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:examples/excel-using-filename.xml")
public class ExcelSpecifiedConfigurationFromFilenameTest {

    @Autowired
    private DataContext dataContext;

    @Test
    public void testReadDataContext() {
        assertEquals(ExcelDataContext.class, dataContext.getClass());

        ExcelDataContext excel = (ExcelDataContext) dataContext;
        Resource resource = excel.getResource();

        assertEquals(FileResource.class, resource.getClass());

        assertEquals("example.xlsx", resource.getName());

        assertEquals("[hello, world]", Arrays.toString(excel.getDefaultSchema().getTable(0).getColumnNames()));

        Assert.assertTrue(excel.getConfiguration().isSkipEmptyLines());
        Assert.assertTrue(excel.getConfiguration().isSkipEmptyColumns());
    }
}

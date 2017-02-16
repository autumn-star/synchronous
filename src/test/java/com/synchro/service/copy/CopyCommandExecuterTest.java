package com.synchro.service.copy;

import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;
import junit.framework.Assert;

import org.junit.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by jinqinghe on 2/13/17.
 */
public class CopyCommandExecuterTest {

    @Test(expected = RuntimeException.class)
    public void testExecuteFailed() throws Exception {
        String cmd = "echo 'hello, world'";
        LinkedBlockingQueue<RowData> queue = new LinkedBlockingQueue<>();
        List<ColumnMetaData > columnMetaDatas = new ArrayList<>();
        CopyCommandExecuter.execute(cmd, queue, columnMetaDatas);
    }

    @Test
    public void testExecuteSucceed() throws Exception {
        String cmd = "echo 'hello#world'";
        LinkedBlockingQueue<RowData> queue = new LinkedBlockingQueue<>();
        List<ColumnMetaData > columnMetaDatas = new ArrayList<>();
        ColumnMetaData metaData1 = new ColumnMetaData("name1", Types.VARCHAR, "VARCHAR");
        ColumnMetaData metaData2 = new ColumnMetaData("name2", Types.VARCHAR, "VARCHAR");
        columnMetaDatas.add(metaData1);
        columnMetaDatas.add(metaData2);
        CopyCommandExecuter.execute(cmd, queue, columnMetaDatas);
        Assert.assertTrue(queue.size() == 1);
    }
}
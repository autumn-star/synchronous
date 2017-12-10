package com.synchro.tool;

import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.BaseWorker;
import com.synchro.worker.PostgresToPostgresWorker;

/**
 * Created by xingxing.duan & liqiu on 2017/8/21.
 * Last modified by liqiu 2017-09-05
 */
public class PostgresToPostgresTool extends SyncTool {

    @Override
    public int run(SyncOptionsDto var1) throws Exception {
        BaseWorker baseWorker = new PostgresToPostgresWorker();
        baseWorker.setOptions(var1);
        baseWorker.run();
        return 0;
    }

    public static void main(String[] args){
        SyncOptionsDto syncOptionsDto = new SyncOptionsDto();
        syncOptionsDto.setSrcDataSourceName("log_analysis");
        syncOptionsDto.setSrcSchemaName("mirror");
        syncOptionsDto.setSrcTableName("b2c_order");
        syncOptionsDto.setTgtDataSourceName("log_analysis");
        syncOptionsDto.setTgtSchemaName("realtime_data");
        syncOptionsDto.setTgtTableName("b2c_order");
        syncOptionsDto.setSplitByColumn("create_time");
        BaseWorker baseWorker = new PostgresToPostgresWorker();
        baseWorker.setOptions(syncOptionsDto);
        baseWorker.run();

    }
}
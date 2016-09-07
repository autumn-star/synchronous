package com.synchro.tool;

import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.BaseWorker;
import com.synchro.worker.RealtimePostgresToPostgresWorker;

/**
 * Created by xingxing.duan on 2015/9/16.
 */
public class RealtimePostgresToPostgresTool extends SyncTool {
    @Override
    public int run(SyncOptionsDto options) throws Exception {
        BaseWorker baseWorker = new RealtimePostgresToPostgresWorker();
        baseWorker.setOptions(options);
        baseWorker.run();
        return 0;
    }

    public static void main(String[] args){
        SyncOptionsDto syncOptionsDto = new SyncOptionsDto();
        syncOptionsDto.setSrcDataSourceName("src_tts_online");
        syncOptionsDto.setSrcSchemaName("public");
        syncOptionsDto.setSrcTableName("b2c_order_view");
        syncOptionsDto.setPartitionColumnName("operate_time");
        syncOptionsDto.setTgtDataSourceName("log_analysis");
        syncOptionsDto.setTgtSchemaName("realtime_data");
        syncOptionsDto.setTgtTableName("b2c_order");
        /*syncOptionsDto.setSplitByColumn("create_time");*/
        BaseWorker baseWorker = new RealtimePostgresToPostgresWorker();
        baseWorker.setOptions(syncOptionsDto);
        baseWorker.run();

    }
}

package com.synchro.tool;

import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.BaseWorker;
import com.synchro.worker.HiveToPostgresWorker;

/**
 * Created by xingxing.duan on 2015/8/13. tool trasform data from hdfs to
 * postgres
 */
public class HiveToPostgresTool extends SyncTool {
	@Override
	public int run(SyncOptionsDto var1) throws Exception {
		BaseWorker baseWorker = new HiveToPostgresWorker();
		baseWorker.setOptions(var1);
		baseWorker.run();
		return 0;
	}
}
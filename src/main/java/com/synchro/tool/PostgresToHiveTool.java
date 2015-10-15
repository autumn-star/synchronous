package com.synchro.tool;

import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.BaseWorker;
import com.synchro.worker.PostgresToHiveWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xingxing.duan & liqiu on 2015/8/13. tool trasform data from postgres to
 */
public class PostgresToHiveTool extends SyncTool {

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncTool.class);

	@Override
	public int run(SyncOptionsDto options) throws Exception {
		//BaseWorker baseWorker = SpringContextUtils.getBean(PostgresToHiveWorker.class);
		BaseWorker baseWorker = new PostgresToHiveWorker();
		LOGGER.info(options.toString());
		baseWorker.setOptions(options);
		baseWorker.run();
		return 1;
	}
}

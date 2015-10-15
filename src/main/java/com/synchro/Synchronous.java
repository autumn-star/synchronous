package com.synchro;

import com.synchro.tool.SyncTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

/**
 * Created by xingxing.duan on 2015/8/12. main entrance for Synchronous
 * @Last modified by liqiu 2015-09-05 Sat.
 */
public class Synchronous {

	private static final Logger LOGGER = LoggerFactory.getLogger(Synchronous.class);

	/**
	 * 入口程序
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length == 0) {
			System.err.println("Try 'sync help' for usage.");
			System.exit(1);
		}
		String toolName = args[0];
		LOGGER.info("get SyncTool:" + toolName);
		SyncTool tool = SyncTool.getTool(toolName); // 获取工具对象
		if (null == tool) {
			System.err.println("No such sync tool: " + toolName + ". See 'sync help'.");
			System.exit(1);
		}

		// 执行同步任务
		int ret = SyncTool.sync(Arrays.copyOfRange(args, 1, args.length), tool);
		System.exit(ret);
	}
}

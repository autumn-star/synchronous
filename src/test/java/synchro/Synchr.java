package synchro;

import javax.annotation.Resource;

import com.synchro.tool.SyncTool;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.PostgresToPostgresWorker;

import java.util.Arrays;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class Synchr {

	private static final Logger LOGGER = LoggerFactory.getLogger(Synchr.class);

	@Test
	@Ignore
	public void synchro() {
		String commond = "pg-to-pg -ss log_analysis -sc mirror -st b2c_product_ticket_date -ts log_analysis_data3 -tc mirror -tt b2c_product_ticket_date";
		String[] args = commond.split(" ");
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

	@Test
	public void test(){
		int i=0;
		System.out.println(i / 0);
	}
}
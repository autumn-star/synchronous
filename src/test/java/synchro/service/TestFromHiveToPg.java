package synchro.service;

import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.HiveToPostgresWorker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class TestFromHiveToPg {

	@Resource
	HiveToPostgresWorker fromHiveToPg;

	@Test
	/*@Ignore*/
	public void getFormHiveToPg() {
		SyncOptionsDto fromPgToHiveDto = new SyncOptionsDto();
		fromPgToHiveDto.setSrcDataSourceName("hive");
		fromPgToHiveDto.setSrcSchemaName("common");
		fromPgToHiveDto.setSrcTableName("com_platform_traffic_d");
		fromPgToHiveDto.setPartitionColumnName("");
		fromPgToHiveDto.setTgtDataSourceName("log_analysis");
		fromPgToHiveDto.setTgtSchemaName("common");
		fromPgToHiveDto.setTgtTableName("com_platform_traffic_d");
		fromPgToHiveDto.setWhere("rpt_date='2015-07-26'");
		//fromPgToHiveDto.setWhere("operate_time='2015-06-21'");
		try {
			fromHiveToPg.setOptions(fromPgToHiveDto);
			fromHiveToPg.run();
		} catch (Exception e) {
			System.out.println(e.toString());
		}
	}
}

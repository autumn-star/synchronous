package synchro;

import javax.annotation.Resource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.worker.PostgresToPostgresWorker;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:spring.xml")
public class TestSynchr {
	
	@Resource
	PostgresToPostgresWorker postgresToPostgresWorker;
	
	@Test
	public void synchro(){
		SyncOptionsDto options = new SyncOptionsDto();
		options.setSrcDataSourceName("src_tts_online");
		options.setSrcSchemaName("public");
		options.setSrcTableName("b2c_card");
		options.setPartitionColumnName("");
		options.setTgtDataSourceName("log_analysis");
		options.setTgtSchemaName("mirror");
		options.setTgtTableName("b2c_card");
		//options.setWhere("id=1");
		//fromPgToHiveDto.setWhere("operate_time='2015-06-21'");
		postgresToPostgresWorker.setOptions(options);
		postgresToPostgresWorker.run();
	}
}
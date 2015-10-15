package synchro;

import com.synchro.service.ColumnAdapterService;

import org.junit.Test;

/**
 * Created by qiu.li on 2015/8/17.
 */
public class SqlTypeAdapterTest {

    @Test
    public void testHstore() {

        System.out.println(ColumnAdapterService.getHstore("\"t\"=>\"1439608430306\", \"cid\"=>\"C1001\", \"clk\"=>\"ap_yud\", \"gid\"=>\"5BA0232C-26E7-1EDA-19EB-E8FBDFAF5652\", \"pid\"=>\"10010\", \"uid\"=>\"5F1D853C-384B-455E-ADDA-CEB89AD96421\", \"vid\"=>\"80011093\", \"utmr_t\"=>\"ticket_activityDetail\", \"in_track\"=>\"5\", \"bd_source\"=>\"iphone\", \"dist_city\"=>\"黄石市\", \"from_area\"=>\"ac_user_collection_list\", \"from_index\"=>\"1\", \"from_value\"=>\"黄冈罗田君怡南山酒店+ 商务标准间+罗田天堂寨成人门票\", \"utmr_value\"=>\"3641943218\", \"current_city\"=>\"黄石\""));
        System.out.println("test");
    }
}

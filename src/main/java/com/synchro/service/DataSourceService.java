package com.synchro.service;

import com.synchro.dal.metadata.DataSourceMetaData;
import com.synchro.util.XmlUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author qiu.li
 * @description 基本数据源管理类
 * @since 2015-03-25
 */
@Service
public class DataSourceService {

    @Value("${dataourcepath}")
    String dataSourcePath;

    /**
     * 获取数据源
     *
     * @return
     */
    public DataSourceMetaData getDataSource(String dataSourceName) {
    	String xmlFile = this.dataSourcePath + dataSourceName + ".xml";
    	if (!XmlUtils.isExist(xmlFile)){
    		xmlFile = "datasource/" + dataSourceName + ".xml";
    	}
    	return XmlUtils.readDataSourceXml(xmlFile);
    }

}

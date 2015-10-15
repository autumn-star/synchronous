package com.synchro.util;

import com.synchro.dal.metadata.DataSourceMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Created by qiu.li on 2015/8/27.
 */
public class XmlUtils {

	private final static Logger logger = LoggerFactory.getLogger(XmlUtils.class);

	/**
	 * 把对象写入xml文件
	 *
	 * @param path
	 * @param dataSourceMetaData
	 */
	public static void writeDataSourceXml(String path, DataSourceMetaData dataSourceMetaData) {
		try {
			JAXBContext context = JAXBContext.newInstance(DataSourceMetaData.class);
			// 下面代码演示将对象转变为xml
			Marshaller m = context.createMarshaller();
			FileWriter fw = new FileWriter(path + dataSourceMetaData.getDataSourceName() + ".xml");
			m.marshal(dataSourceMetaData, fw);
			fw.close();
		} catch (Exception e) {
			logger.error(e.toString());
		}
	}

	/**
	 * 从文件读取xml到对象
	 *
	 * @param file
	 * @return
	 */
	public static DataSourceMetaData readDataSourceXml(String file) {
		DataSourceMetaData dataSourceMetaData = new DataSourceMetaData();
		try {
			JAXBContext context = JAXBContext.newInstance(DataSourceMetaData.class);
			// 下面代码演示将上面生成的xml转换为对象
			FileReader fr = new FileReader(file);

			Unmarshaller um = context.createUnmarshaller();
			dataSourceMetaData = (DataSourceMetaData) um.unmarshal(fr);
			fr.close();
		} catch (Exception e) {
			logger.error("readDataSourceXml faild",e);
		}
		return dataSourceMetaData;
	}

	/**
	 * 判断文件是否存在
	 * @param path
	 */
	public static boolean isExist(String path) {
		File file = new File(path);
		// 判断文件夹是否存在,如果不存在则创建文件夹
		if (!file.exists()) {
			return false;
		}
		return true;
	}

}

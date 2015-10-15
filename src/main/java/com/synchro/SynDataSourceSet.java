package com.synchro;

import com.synchro.dal.metadata.DataBaseTypeMetaData;
import com.synchro.dal.metadata.DataSourceMetaData;
import com.synchro.util.XmlUtils;

/**
 * Created by liqiu on 15/8/29.
 */
public class SynDataSourceSet {

    public static void main(String[] args) {
        DataSourceMetaData logAnalysisDs = new DataSourceMetaData();
        if (args.length < 6) {
            System.out.println("please input args like: name databaseUrl port databasename username password xmlpath");
            System.exit(1);
        }
        logAnalysisDs.setDataBaseType(DataBaseTypeMetaData.PostgreSQL);
        logAnalysisDs.setDataSourceName(args[0]);
        logAnalysisDs.setIp(args[1]);
        logAnalysisDs.setPort(Integer.valueOf(args[2]));
        logAnalysisDs.setDatabaseName(args[3]);
        logAnalysisDs.setUserName(args[4]);
        logAnalysisDs.setPassword(args[5]);
        XmlUtils.writeDataSourceXml(args[6], logAnalysisDs);
    }
}
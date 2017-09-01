package com.synchro.dal.metadata;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author qiu.li
 * @description 基本数据源类型
 * @since 2015-03-25
 */
@XmlRootElement(name = "DataBaseTypeMetaData")
public enum DataBaseTypeMetaData {
    Oracle1("Oracle", "oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@%s:%d:%s"),
    PostgreSQL("PostgreSQL", "org.postgresql.Driver", "jdbc:postgresql://%s:%d/%s?useUnicode=true&amp;characterEncoding=utf8"),
    MySQL("MySQL", "com.mysql.jdbc.Driver", "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8"),
    HIVE("HIVE", "org.apache.hive.jdbc.HiveDriver", "jdbc:hive2://%s:%d/default");

    private String databaseName;

    private String driverClass;

    private String formatUrl;

    DataBaseTypeMetaData() {

    }

    private DataBaseTypeMetaData(String databaseName, String driverClass, String formatUrl) {
        this.databaseName = databaseName;
        this.driverClass = driverClass;
        this.formatUrl = formatUrl;
    }

    @XmlAttribute
    public String getDatabaseName() {
        return this.databaseName;
    }

    @XmlAttribute
    public String getDriverClass() {
        return driverClass;
    }

    @XmlAttribute
    public String getFormatUrl() {
        return this.formatUrl;
    }

    public void setFormatUrl(String formatUrl) {
        this.formatUrl = formatUrl;
    }

    public void setDriverClass(String driverClass) {

        this.driverClass = driverClass;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}

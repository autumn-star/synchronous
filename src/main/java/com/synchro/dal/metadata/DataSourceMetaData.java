package com.synchro.dal.metadata;


import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * 数据源设置类
 *
 * @author qiu.li
 */
@XmlRootElement(name = "DataSourceMetaData")
public class DataSourceMetaData {


    private String dataSourceName; // 数据源名称

    private DataBaseTypeMetaData dataBaseType; // 数据源类型

    private String ip; // 数据源ip或者url

    private int port; // 端口

    private String databaseName; // 数据库名称

    private String userName; // 用户名

    private String password; // 密码

    public DataSourceMetaData() {

    }

    public DataSourceMetaData(String datasourceName, DataBaseTypeMetaData databaseType, String ip, int port, String databaseName, String userName, String password) {
        this.dataSourceName = datasourceName;
        this.dataBaseType = databaseType;
        this.ip = ip;
        this.port = port;
        this.databaseName = databaseName;
        this.userName = userName;
        this.password = password;
    }

    @XmlAttribute
    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    @XmlElement
    public DataBaseTypeMetaData getDataBaseType() {
        return dataBaseType;
    }

    public void setDataBaseType(DataBaseTypeMetaData dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    @XmlAttribute
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @XmlAttribute
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @XmlAttribute
    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    @XmlAttribute
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @XmlAttribute
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }


    public String getConnectionUrl() {
        if (this.dataBaseType == DataBaseTypeMetaData.HIVE) {
            return String.format(this.dataBaseType.getFormatUrl(), this.ip, this.port);
        }
        return String.format(this.dataBaseType.getFormatUrl(), this.ip, this.port, this.databaseName);
    }

}

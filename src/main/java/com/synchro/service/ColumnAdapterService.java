package com.synchro.service;

import com.synchro.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;

/**
 * 类型对应关系类
 *
 * @author qiu.li
 *
 */
public class ColumnAdapterService {
    private final static Logger logger = LoggerFactory.getLogger(ColumnAdapterService.class);
    private final static String DIVIDE = ",";
    private final static String HIVE_EQURE = "=";
    private final static String PG_EQURE = "=>";

    /**
     * 赋值
     *
     * @param preparedStmt 游标
     * @param index        位置
     * @param value        值
     * @param type         类型
     * @throws Exception
     */
    public static void setParameterValue(PreparedStatement preparedStmt, int index, Object value, int type) throws Exception {
        try {
            if (value == null || value.toString().equals("\\N")) {
                preparedStmt.setObject(index, null);
                return;
            }
            switch (type) {
                case Types.ARRAY:
                    preparedStmt.setArray(index, (Array) value);
                    break;
                case Types.BIGINT:
                    preparedStmt.setLong(index, Long.parseLong(value.toString()));
                    break;
                case Types.BINARY:
                    preparedStmt.setBytes(index, value.toString().getBytes());
                    break;
                case Types.BIT:
                    preparedStmt.setBoolean(index, (Boolean.parseBoolean(value.toString())));
                    break;
                case Types.BLOB:
                    preparedStmt.setBlob(index, (Blob) value);
                    break;
                case Types.BOOLEAN:
                    preparedStmt.setBoolean(index, (Boolean.valueOf(value.toString())));
                    break;
                case Types.CHAR:
                    preparedStmt.setString(index, (value.toString()));
                    break;
                case Types.CLOB:
                    preparedStmt.setClob(index, (Clob) value);
                    break;
                case Types.DATALINK:
                    break;
                case Types.DATE:
                    preparedStmt.setDate(index, DateUtils.stringToSqlDate(value.toString()));
                    break;
                case Types.DECIMAL:
                    preparedStmt.setBigDecimal(index, (BigDecimal.valueOf(Double.parseDouble(value.toString()))));
                    break;
                case Types.DISTINCT:
                    break;
                case Types.DOUBLE:
                    preparedStmt.setDouble(index, (Double.valueOf(value.toString())));
                    break;
                case Types.FLOAT:
                    preparedStmt.setFloat(index, (Float.valueOf(value.toString())));
                    break;
                case Types.INTEGER:
                    preparedStmt.setInt(index, (Integer.valueOf(value.toString())));
                    break;
                case Types.JAVA_OBJECT:
                    preparedStmt.setObject(index, value);
                    break;
                case Types.LONGNVARCHAR:
                    break;
                case Types.LONGVARBINARY:
                    break;
                case Types.LONGVARCHAR:
                    preparedStmt.setString(index, ((String) value.toString()));
                    break;
                case Types.NCHAR:
                    break;
                case Types.NCLOB:
                    break;
                case Types.NULL:
                    break;
                case Types.NUMERIC:
                    preparedStmt.setBigDecimal(index, (BigDecimal.valueOf(Double.parseDouble(value.toString()))));
                    break;
                case Types.NVARCHAR:
                    break;
                case Types.REAL:
                    break;
                case Types.REF:
                    break;
                case Types.ROWID:
                    break;
                case Types.SMALLINT:
                    preparedStmt.setInt(index, (Integer.valueOf(value.toString())));
                    break;
                case Types.SQLXML:
                    break;
                case Types.STRUCT:
                    break;
                case Types.TIME:
                    preparedStmt.setTime(index, (Time.valueOf(value.toString())));
                    break;
                case Types.TIMESTAMP:
                    preparedStmt.setTimestamp(index, DateUtils.getTimestamp(value.toString()));
                    break;
                case Types.TINYINT:
                    preparedStmt.setInt(index, (Integer.parseInt(value.toString())));
                    break;
                case Types.VARBINARY:
                    break;
                case Types.VARCHAR:
                    preparedStmt.setString(index, ((String) value));
                    break;
                case Types.OTHER:
                    if (value != null&&value.toString().length()>1) {
                        if (value.toString().substring(0, 1).equals("{")) {
                            // hstore类型数据特殊处理
                            value = getHstore(value.toString());
                        }
                    }

                    preparedStmt.setObject(index, value, Types.OTHER);

                    break;
                default:
                    preparedStmt.setObject(index, value);
                    break;
            }
        } catch (Exception e) {
            logger.error("int " + index + ", Object " + value + ", int " + type + " setParameterValue error" + e.toString());
            throw e;
        }
    }

    /**
     * 赋值
     *
     * @param preparedStmt 游标
     * @param index        位置
     * @param value        值
     * @param type         类型
     * @throws Exception
     */
    public static void setParameterValueWithPg(PreparedStatement preparedStmt, int index, Object value, int type) throws Exception {
        try {
            if (value == null) {
                preparedStmt.setObject(index, null);
                return;
            }
            switch (type) {
                case Types.ARRAY:
                    preparedStmt.setArray(index, (Array) value);
                    break;
                case Types.BIGINT:
                    preparedStmt.setLong(index, Long.parseLong(value.toString()));
                    break;
                case Types.BINARY:
                    preparedStmt.setBytes(index, value.toString().getBytes());
                    break;
                case Types.BIT:
                    preparedStmt.setBoolean(index, (Boolean.parseBoolean(value.toString())));
                    break;
                case Types.BLOB:
                    preparedStmt.setBlob(index, (Blob) value);
                    break;
                case Types.BOOLEAN:
                    preparedStmt.setBoolean(index, (Boolean.valueOf(value.toString())));
                    break;
                case Types.CHAR:
                    preparedStmt.setString(index, (value.toString()));
                    break;
                case Types.CLOB:
                    preparedStmt.setClob(index, (Clob) value);
                    break;
                case Types.DATALINK:
                    break;
                case Types.DATE:
                    preparedStmt.setDate(index, DateUtils.stringToSqlDate(value.toString()));
                    break;
                case Types.DECIMAL:
                    preparedStmt.setBigDecimal(index, (BigDecimal.valueOf(Double.parseDouble(value.toString()))));
                    break;
                case Types.DISTINCT:
                    break;
                case Types.DOUBLE:
                    preparedStmt.setDouble(index, (Double.valueOf(value.toString())));
                    break;
                case Types.FLOAT:
                    preparedStmt.setFloat(index, (Float.valueOf(value.toString())));
                    break;
                case Types.INTEGER:
                    preparedStmt.setInt(index, (Integer.valueOf(value.toString())));
                    break;
                case Types.JAVA_OBJECT:
                    preparedStmt.setObject(index, value);
                    break;
                case Types.LONGNVARCHAR:
                    break;
                case Types.LONGVARBINARY:
                    break;
                case Types.LONGVARCHAR:
                    preparedStmt.setString(index, ((String) value.toString()));
                    break;
                case Types.NCHAR:
                    break;
                case Types.NCLOB:
                    break;
                case Types.NULL:
                    break;
                case Types.NUMERIC:
                    preparedStmt.setBigDecimal(index, (BigDecimal.valueOf(Double.parseDouble(value.toString()))));
                    break;
                case Types.NVARCHAR:
                    break;
                case Types.REAL:
                    break;
                case Types.REF:
                    break;
                case Types.ROWID:
                    break;
                case Types.SMALLINT:
                    preparedStmt.setInt(index, (Integer.valueOf(value.toString())));
                    break;
                case Types.SQLXML:
                    break;
                case Types.STRUCT:
                    break;
                case Types.TIME:
                    preparedStmt.setTime(index, (Time.valueOf(value.toString())));
                    break;
                case Types.TIMESTAMP:
                    preparedStmt.setTimestamp(index, DateUtils.getTimestamp(value.toString()));
                    break;
                case Types.TINYINT:
                    preparedStmt.setInt(index, (Integer.parseInt(value.toString())));
                    break;
                case Types.VARBINARY:
                    break;
                case Types.VARCHAR:
                    preparedStmt.setString(index, ((String) value));
                    break;
                case Types.OTHER:
                    preparedStmt.setObject(index, value, Types.OTHER);
                    break;
                default:
                    preparedStmt.setObject(index, value);
                    break;
            }
        } catch (Exception e) {
            logger.error("int " + index + ", Object " + value + ", int " + type + " setParameterValue error" + e.toString());
            throw e;
        }
    }

    /**
     * 将Hive的Map转换成Pg的HSTore
     *
     * @param str
     * @return
     */
    public static String getHstore(String str) {
        //logger.info(str);
        str = str.substring(1, str.length() - 1).trim().replace('\\', ' ').replace(" ", "").replace("\"","");
        String[] tmp = str.split(ColumnAdapterService.DIVIDE);
        StringBuffer sbf = new StringBuffer();
        for (int i = 0; i < tmp.length; i++) {
            String[] m = tmp[i].split(ColumnAdapterService.HIVE_EQURE);
            if (m.length == 2 && m[0].trim().length() >= 1 && m[1].trim().length() >= 1) {
                sbf.append("\"" + m[0] + "\" " + ColumnAdapterService.PG_EQURE + "\"" + m[1] + "\"" + ColumnAdapterService.DIVIDE);
            }
        }
        //logger.info(sbf.substring(0, sbf.length() - 1).toString());
        return sbf.length() > 4 ? sbf.substring(0, sbf.length() - 1).toString() : "";
    }

    /**
     * 根据类型获取名称
     *
     * @param type
     * @return
     * @throws SQLException
     */
    public static String getTypeName(int type) throws SQLException {
        switch (type) {
            case Types.ARRAY:
                break;
            case Types.BIGINT:
                return "BIGINT";
            case Types.BINARY:
                return "BINARY";
            case Types.BIT:
                return "BIT";
            case Types.BLOB:
                return "BLOB";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.CHAR:
                return "CHAR";
            case Types.CLOB:
                return "CLOB";
            case Types.DATALINK:
                return "DATALINK";
            case Types.DATE:
                return "DATE";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.DISTINCT:
                return "DISTINCT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.FLOAT:
                return "FLOAT";
            case Types.INTEGER:
                return "INTEGER";
            case Types.JAVA_OBJECT:
                break;
            case Types.LONGNVARCHAR:
                return "LONGNVARCHAR";
            case Types.LONGVARBINARY:
                return "LONGVARBINARY";
            case Types.LONGVARCHAR:
                return "LONGVARCHAR";
            case Types.NCHAR:
                return "NCHAR";
            case Types.NCLOB:
                return "NCLOB";
            case Types.NULL:
                return "NULL";
            case Types.NUMERIC:
                return "NUMERIC";
            case Types.NVARCHAR:
                return "NVARCHAR";
            case Types.REAL:
                return "REAL";
            case Types.REF:
                return "REF";
            case Types.ROWID:
                return "ROWID";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.SQLXML:
                break;
            case Types.STRUCT:
                break;
            case Types.TIME:
                return "TIME";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.TINYINT:
                return "TINYINT";
            case Types.VARBINARY:
                return "VARBINARY";
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.OTHER:
                break;
            default:
                break;
        }
        return "VARCHAR";
    }

    public static void main(String[] args) {
        System.out.println(ColumnAdapterService.getHstore("\"t\"=>\"1439608430306\", \"cid\"=>\"C1001\", \"clk\"=>\"ap_yud\", \"gid\"=>\"5BA0232C-26E7-1EDA-19EB-E8FBDFAF5652\", \"pid\"=>\"10010\", \"uid\"=>\"5F1D853C-384B-455E-ADDA-CEB89AD96421\", \"vid\"=>\"80011093\", \"utmr_t\"=>\"ticket_activityDetail\", \"in_track\"=>\"5\", \"bd_source\"=>\"iphone\", \"dist_city\"=>\"黄石市\", \"from_area\"=>\"ac_user_collection_list\", \"from_index\"=>\"1\", \"from_value\"=>\"黄冈罗田君怡南山酒店+ 商务标准间+罗田天堂寨成人门票\", \"utmr_value\"=>\"3641943218\", \"current_city\"=>\"黄石\""));

        System.out.println("t".length());
    }

}

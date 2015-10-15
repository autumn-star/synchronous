package com.synchro.util;

import com.synchro.dal.condition.TableMetaDataCondition;
import org.apache.commons.lang.StringUtils;

/**
 * Created by xingxing.duan on 2015/8/23.
 */
public class SqlUtils {

    /**
     * 获取源带有所有字段的select语句
     *
     * @param tableMetaDataCondition
     * @return
     */
    public static String getSelectQuery(TableMetaDataCondition tableMetaDataCondition) {
        StringBuilder querySql = new StringBuilder();
        String columns = StringUtils.isBlank(tableMetaDataCondition.getColumns()) ? "*" : tableMetaDataCondition.getColumns();
        querySql.append("SELECT " + columns + " FROM ").append(tableMetaDataCondition.getSchema()).append(".").append(tableMetaDataCondition.getTableName());

        /*if (StringUtils.isNotBlank(tableMetaDataCondition.getWhere())) {
            querySql.append(" where " + tableMetaDataCondition.getWhere());
        }*/
        return querySql.toString();
    }
}

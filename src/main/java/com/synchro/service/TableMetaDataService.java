package com.synchro.service;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.synchro.common.constant.DatabaseType;
import com.synchro.dal.condition.TableMetaDataCondition;
import com.synchro.dal.dao.HiveDao;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.dal.libs.JdbcTemplateFactory;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.TableMetaData;
import com.synchro.util.SqlUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSetMetaData;
import org.springframework.stereotype.Service;

import java.sql.Types;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author qiu.li
 * @Createtime 2015-05-23
 * @Info 数据表Meta信息处理
 */
@Service
public class TableMetaDataService {

    private final static Logger logger = LoggerFactory.getLogger(TableMetaDataService.class);

    private final static Joiner joiner = Joiner.on(",");

    @Autowired
    HiveDao hiveDao;


    /**
     * 获取PostgreSql数据表Meta信息
     *
     * @param tableMetaDataCondition
     * @return
     */
    public TableMetaData getTableMetaData(TableMetaDataCondition tableMetaDataCondition, DatabaseType databaseType) {
        TableMetaData tableMetaData = tableMetaDataCondition.convert();
        JdbcTemplate jdbcTemplate = JdbcTemplateFactory.getInstance().getJdbcTemplateFromDataSourceName(tableMetaDataCondition.getDatasource());
        if (databaseType == DatabaseType.POSTGRES) {
            String selectSql = SqlUtils.getSelectQuery(tableMetaDataCondition);

            SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(selectSql + " LIMIT 0");
            tableMetaData.setColumnMetaDatas(getColumnMetaData(tableMetaDataCondition, sqlRowSet));
        } else {
            if (!hiveDao.checkTableExit(jdbcTemplate, tableMetaDataCondition.getSchema(), tableMetaDataCondition.getTableName())) {
                // 如果数据表不存在
                return null;
            }
            tableMetaData.setColumnMetaDatas(hiveDao.getColumnMetaData(jdbcTemplate, tableMetaDataCondition.getSchema(), tableMetaDataCondition.getTableName()));
        }

        return tableMetaData;
    }


    public static List<ColumnMetaData> getColumnMetaData(TableMetaDataCondition tableMetaDataCondition, SqlRowSet sqlRowSet) {
        List<ColumnMetaData> columneMetaList = new LinkedList<ColumnMetaData>();
        int columnCount;
        Set set = Sets.newHashSet();
        String excludeColumns = tableMetaDataCondition.getExcludeColumns();

        if (StringUtils.isNotBlank(excludeColumns)) {
            String[] ecColumns = excludeColumns.split(",");
            for (String column : ecColumns) {
                set.add(column);
            }
        }
        SqlRowSetMetaData sqlRowSetMetaData = sqlRowSet.getMetaData();
        columnCount = sqlRowSetMetaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {

            if (set.contains(sqlRowSetMetaData.getColumnName(i))) {
                continue;
            }
            if (sqlRowSetMetaData.getColumnType(i) < Types.ARRAY) {
                columneMetaList.add(new ColumnMetaData(sqlRowSetMetaData.getColumnName(i), sqlRowSetMetaData.getColumnType(i), sqlRowSetMetaData.getColumnTypeName(i)));
            } else {
                // 特殊类型暂不处理
                logger.info("特殊类型：" + sqlRowSetMetaData.getColumnName(i) + sqlRowSetMetaData.getColumnType(i) + sqlRowSetMetaData.getColumnTypeName(i));
                // columneMetaList.add(new ColumnMetaData(sqlRowSetMetaData.getColumnName(i), sqlRowSetMetaData.getColumnType(i), sqlRowSetMetaData.getColumnTypeName(i)));
            }
        }
        // 字段排序
        Collections.sort(columneMetaList);
        return columneMetaList;
    }


    /**
     * 获取一个数据表的所有column的sql语句，包括where部分
     *
     * @param tableMetaData
     * @param parameterDto
     * @return
     */
    public String getColumnSql(TableMetaData tableMetaData, SyncOptionsDto parameterDto) {
        StringBuilder sql = new StringBuilder(" select ");
        // 字段列表
        List<String> columns = Lists.transform(tableMetaData.getColumnMetaDatas(), new Function<ColumnMetaData, String>() {
            @Override
            public String apply(ColumnMetaData input) {
                return input.getName();
            }
        });
        sql.append(joiner.join(columns));
        sql.append(" from ").append(parameterDto.getSrcSchemaName()).append(".").append(parameterDto.getSrcTableName());
        // 按条件同步
        if (StringUtils.isNotBlank(parameterDto.getWhere())) {
            sql.append(" where " + parameterDto.getWhere());
        }
        return sql.toString();
    }
}

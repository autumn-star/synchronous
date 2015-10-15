package com.synchro.dal.dto;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xingxing.duan
 * Date: 14-4-29
 * Time: 下午7:35
 * 执行sql的上下文内容
 */
public class SqlContextDto {

    /**
     * 执行的sql
     */
    private StringBuilder sql;

    /**
     * 参数，对应sql中的?号
     */
    private List<Object> params;


    public SqlContextDto(StringBuilder sql, List<Object> params) {
        this.sql = sql;
        this.params = params;
    }

    public StringBuilder getSql() {
        return sql;
    }

    public void setSql(StringBuilder sql) {
        this.sql = sql;
    }

    public List<Object> getParams() {
        return params;
    }

    public void setParams(List<Object> params) {
        this.params = params;
    }
}
package com.synchro.io.split;

import com.google.common.collect.Lists;
import com.synchro.common.constant.SyncConstant;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by xingxing.duan on 2015/8/17.
 * splitter data by integer cloumn
 */
public class IntegerSplitter implements InputSplitter {


    @Override
    public List<InputSplit> split(SqlRowSet sqlRowSet, String colName) throws SQLException {

        long minVal = sqlRowSet.getLong(1);
        long maxVal = sqlRowSet.getLong(2);

        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";
        List<InputSplit> inputSplits = Lists.newArrayList();

        for (long i = minVal; i < maxVal; i = i + SyncConstant.PG_TO_HIVE_THRESHOLD) {

            inputSplits.add(new InputSplit(lowClausePrefix + Long.toString(i), highClausePrefix + (i + SyncConstant.PG_TO_HIVE_THRESHOLD)));
        }

        return inputSplits;

    }
}

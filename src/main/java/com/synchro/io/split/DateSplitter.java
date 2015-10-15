package com.synchro.io.split;

import com.google.common.collect.Lists;
import com.synchro.util.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.List;

/**
 * Created by xingxing.duan on 2015/8/17.
 * splitter data by date cloumn
 */
public class DateSplitter implements InputSplitter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DateSplitter.class);

    @Override
    public List<InputSplit> split(SqlRowSet  sqlRowSet, String colName) throws SQLException {

        List<InputSplit> splits = Lists.newArrayList();

        long minVal;
        long maxVal;

        int sqlDataType = sqlRowSet.getMetaData().getColumnType(1);
        minVal = resultSetColToLong(sqlRowSet, 1, sqlDataType);
        maxVal = resultSetColToLong(sqlRowSet, 2, sqlDataType);

        String lowClausePrefix = colName + " >= ";
        String highClausePrefix = colName + " < ";

        List<Date> dates = DateUtils.getDates(longToDate(minVal, sqlDataType), longToDate(maxVal, sqlDataType));

        for (Date date : dates) {
            splits.add(new InputSplit(lowClausePrefix + dateToString(date), highClausePrefix + dateToString(DateUtils.addDay(date,1))));
        }
        return splits;

    }

    private String dateToString(Date d) {
        return "'" + DateUtils.dateToString(d) + "'";
    }

    private Date longToDate(long val, int sqlDataType) {
        switch (sqlDataType) {
            case Types.DATE:
                return new java.sql.Date(val);
            case Types.TIME:
                return new java.sql.Time(val);
            case Types.TIMESTAMP:
                return new java.sql.Timestamp(val);
            default: // Shouldn't ever hit this case.
                return null;
        }
    }


    private long resultSetColToLong(SqlRowSet rs, int colNum, int sqlDataType)
            throws SQLException {
        try {
            switch (sqlDataType) {
                case Types.DATE:
                    return rs.getDate(colNum).getTime();
                case Types.TIME:
                    return rs.getTime(colNum).getTime();
                case Types.TIMESTAMP:
                    return rs.getTimestamp(colNum).getTime();
                default:
                    throw new SQLException("Not a date-type field");
            }
        } catch (NullPointerException npe) {
            // null column. return minimum long value.
            LOGGER.warn("Encountered a NULL date in the split column. "
                    + "Splits may be poorly balanced.");
            return Long.MIN_VALUE;
        }
    }

}

package com.synchro.io.split;

import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by xingxing.duan on 2015/8/17.
 *
 */
public interface InputSplitter {

    /**
     * Given a SqlRowSet containing one record
     * with two columns (a low value, and a high value, both of the same
     * type), determine a set of splits that span the given values.
     */
    List<InputSplit> split(SqlRowSet sqlRowSet, String colName)
            throws SQLException;
}

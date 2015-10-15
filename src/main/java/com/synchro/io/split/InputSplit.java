package com.synchro.io.split;

/**
 * Created by xingxing.duan on 2015/8/17.
 */
public class InputSplit {

    public InputSplit() {
    }

    public InputSplit(String lowerBoundClause, String upperBoundClause) {
        this.lowerBoundClause = lowerBoundClause;
        this.upperBoundClause = upperBoundClause;
    }

    private String lowerBoundClause;
    private String upperBoundClause;

    public String getLowerBoundClause() {
        return lowerBoundClause;
    }

    public void setLowerBoundClause(String lowerBoundClause) {
        this.lowerBoundClause = lowerBoundClause;
    }

    public String getUpperBoundClause() {
        return upperBoundClause;
    }

    public void setUpperBoundClause(String upperBoundClause) {
        this.upperBoundClause = upperBoundClause;
    }
}

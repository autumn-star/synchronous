package com.synchro.option;

import org.apache.commons.cli.Options;

/**
 * Created by xingxing.duan on 2015/8/13.
 */
public class RelatedOptions extends Options {

	private static final long serialVersionUID = 1L;
	
	private String title;

    public RelatedOptions() {
        this("");
    }

    public RelatedOptions(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }
}

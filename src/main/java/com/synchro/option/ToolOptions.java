package com.synchro.option;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Info 获取参数的集合对象 Created by xingxing.duan on 2015/8/14.
 */
public class ToolOptions implements Iterable<RelatedOptions> {

	private List<RelatedOptions> optGroups;

	public ToolOptions() {
		this.optGroups = new ArrayList<RelatedOptions>();
	}

	public void addOptions(RelatedOptions opts) {
		optGroups.add(opts);
	}

	public void addUniqueOptions(RelatedOptions opts) {
		if (!containsGroup(opts.getTitle())) {
			optGroups.add(opts);
		}
	}

	public boolean containsGroup(String title) {
		for (RelatedOptions related : this) {
			if (related.getTitle().equals(title)) {
				return true;
			}
		}

		return false;
	}

	public Iterator<RelatedOptions> iterator() {
		return optGroups.iterator();
	}

	public Options merge() {
		Options mergedOpts = new Options();
		for (RelatedOptions relatedOpts : this) {
			for (Object optObj : relatedOpts.getOptions()) {
				Option opt = (Option) optObj;
				mergedOpts.addOption(opt);
			}
		}

		return mergedOpts;
	}

	public void printHelp() {
		printHelp(new HelpFormatter());
	}

	public void printHelp(HelpFormatter formatter) {
		printHelp(formatter, new PrintWriter(System.out, true));
	}

	public void printHelp(HelpFormatter formatter, PrintWriter pw) {
		boolean first = true;
		for (RelatedOptions optGroup : optGroups) {
			if (!first) {
				pw.println("");
			}
			pw.println(optGroup.getTitle() + ":");
			formatter.printOptions(pw, formatter.getWidth(), optGroup, 0, 4);
			first = false;
		}
	}

	@Override
	public String toString() {
		StringWriter sw = new StringWriter();
		printHelp(new HelpFormatter(), new PrintWriter(sw));
		sw.flush();
		return sw.getBuffer().toString();
	}
}

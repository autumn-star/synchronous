package com.synchro.tool;

import com.google.common.collect.Maps;
import com.synchro.common.constant.HiveDivideConstant;
import com.synchro.common.constant.SyncroModeEnum;
import com.synchro.dal.dto.SyncOptionsDto;
import com.synchro.option.RelatedOptions;
import com.synchro.option.ToolOptions;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by xingxing.duan on 2015/8/12. base tool for Synchronous, it support
 * some basic argment and method; all tool must be extends this class
 */
public abstract class SyncTool {

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncTool.class);

	/**
	 * 元数据说明
	 */
	public static final String SRC_DATASOURCE_ARG = "ss";
	public static final String SRC_SCHEMA_ARG = "sc";
	public static final String SRC_TABLE_ARG = "st";
	public static final String TGT_DATASOURCE_ARG = "ts";
	public static final String TGT_SEHCMA_ARG = "tc";
	public static final String TGT_TABLE_ARG = "tt";

	/**
	 * 字段
	 */
	public static final String PARTITION_COLUMN_NAME_ARG = "c";
	public static final String PARTITION_COLUMN_VALUE_ARG = "cv";
	public static final String COLUMNS_ARG = "cs";
	public static final String EXCLUDE_COLUMNS_ARG = "exclude-columns";
	public static final String SPLIT_COLUMN_ARG = "split-by";
	public static final String PARTITION_FIELD_ARG = "pv";

	/**
	 * where条件
	 */
	public static final String WHERE_ARG = "w";

	/**
	 * 同步方式&队列大小
	 */
	public static final String SYNC_MODE_ARG = "sm";
	public static final String DIRECT_ARG = "direct";
	public static final String QUEUE_SIZE_ARG = "queue-size";

	/**
	 * help
	 */
	public static final String HELP_ARG = "help";

	/**
	 * 字段分隔符
	 */
	public static final String COLUMN_DIVIDE = "column-divide";

	/**
	 * The name of the current tool.
	 */
	private String toolName;

	/**
	 * 注册工具
	 */
	private static final Map<String, Class<? extends SyncTool>> TOOLS;

	static {

		TOOLS = Maps.newHashMap();
		registTool("pg-to-hive", PostgresToHiveTool.class);
		registTool("hive-to-pg", HiveToPostgresTool.class);
		registTool("pg-to-pg", PostgresToPostgresTool.class);
		registTool("realtime-pg-to-pg",RealtimePostgresToPostgresTool.class);
	}

	public SyncTool() {
		this.toolName = "<" + this.getClass().getName() + ">";
	}

	public SyncTool(String name) {
		this.toolName = name;
	}

	public String getToolName() {
		return this.toolName;
	}

	protected void setToolName(String name) {
		this.toolName = name;
	}

	/**
	 * Add a tool to the available set of SyncTool instances.
	 *
	 * @param toolName
	 *            the name the user access the tool through.
	 * @param cls
	 *            the class providing the tool.
	 */
	private static void registTool(String toolName, Class<? extends SyncTool> cls) {

		TOOLS.put(toolName, cls);
	}

	/**
	 * Configure the command-line arguments we expect to receive.
	 *
	 * @param opts
	 *            a ToolOptions that should be
	 */
	public void configureOptions(ToolOptions opts){
		opts.addUniqueOptions(getCommonOptions());
	}

	/**
	 * @return
	 */
	protected RelatedOptions getCommonOptions() {
		RelatedOptions opts = new RelatedOptions("Common Options");
		opts.addOption(OptionBuilder.withArgName(HELP_ARG).hasArg().withDescription("help").withLongOpt(HELP_ARG).create());
		opts.addOption(OptionBuilder.withArgName(SRC_DATASOURCE_ARG).hasArg().withDescription("source datasource name").withLongOpt(SRC_DATASOURCE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(SRC_SCHEMA_ARG).hasArg().withDescription("source schema name").withLongOpt(SRC_SCHEMA_ARG).create());
		opts.addOption(OptionBuilder.withArgName(SRC_TABLE_ARG).hasArg().withDescription("source table name").withLongOpt(SRC_TABLE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(COLUMNS_ARG).hasArg().withDescription("source table column names").withLongOpt(COLUMNS_ARG).create());
		opts.addOption(OptionBuilder.withArgName(TGT_DATASOURCE_ARG).hasArg().withDescription("tgt datasource name").withLongOpt(TGT_DATASOURCE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(TGT_SEHCMA_ARG).hasArg().withDescription("tgt schema name").withLongOpt(TGT_SEHCMA_ARG).create());
		opts.addOption(OptionBuilder.withArgName(TGT_TABLE_ARG).hasArg().withDescription("tgt table name").withLongOpt(TGT_TABLE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(WHERE_ARG).hasArg().withDescription("WHERE clause to use during import").withLongOpt(WHERE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(QUEUE_SIZE_ARG).hasArg().withDescription("queue size").withLongOpt(QUEUE_SIZE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(PARTITION_COLUMN_NAME_ARG).hasArg().withDescription("hive partition column name").withLongOpt(PARTITION_COLUMN_NAME_ARG).create());
		opts.addOption(OptionBuilder.withArgName(PARTITION_COLUMN_VALUE_ARG).hasArg().withDescription("hive partition column value").withLongOpt(PARTITION_COLUMN_VALUE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(SYNC_MODE_ARG).hasArg().withDescription("sync mode").withLongOpt(SYNC_MODE_ARG).create());
		opts.addOption(OptionBuilder.withArgName(SPLIT_COLUMN_ARG).hasArg().withDescription("split column").withLongOpt(SPLIT_COLUMN_ARG).create());
		opts.addOption(OptionBuilder.withArgName(EXCLUDE_COLUMNS_ARG).hasArg().withDescription("exclude column").withLongOpt(EXCLUDE_COLUMNS_ARG).create());
		opts.addOption(OptionBuilder.withArgName(DIRECT_ARG).hasArg().withDescription("direct").withLongOpt(DIRECT_ARG).create());
		opts.addOption(OptionBuilder.withArgName(COLUMN_DIVIDE).hasArg().withDescription("column-divide").withLongOpt(COLUMN_DIVIDE).create());
		return opts;
	}

	/**
	 * 从命令行解析参数到options对象
	 * 
	 * @param args
	 * @param options
	 * @return
	 * @throws ParseException
	 */
	public SyncOptionsDto parseArguments(String[] args, SyncOptionsDto options) throws ParseException {
		ToolOptions toolOptions = new ToolOptions(); // 参数集合
		configureOptions(toolOptions); // 配置需要的参数
		CommandLineParser parser = new BasicParser();
		CommandLine cmdLine = parser.parse(toolOptions.merge(), args, true); // 从命令行参数获取需要的参数
		applyOptions(cmdLine, options); // 将cmdLine导入到options里
		return options;
	}

	/**
	 * Generate the SyncOptions containing actual argument values from the
	 * extracted CommandLine arguments.
	 *
	 * @param in
	 *            the CLI CommandLine that contain the user's set Options.
	 * @param out
	 *            the SyncOptions with all fields applied.
	 */
	protected void applyOptions(CommandLine in, SyncOptionsDto out){
		if (in.hasOption(HELP_ARG)) {
			ToolOptions opts = new ToolOptions();
			this.configureOptions(opts);
			printHelp(opts);
		}
		applyCommonOptions(in, out);
	}

	/**
	 * Configure the command-line common arguments we expect to receive.
	 *
	 * @param in
	 *            the CLI CommandLine that contain the user's set Options.
	 * @param out
	 *            the SyncOptions with all fields applied.
	 */
	protected void applyCommonOptions(CommandLine in, SyncOptionsDto out) {

		if (in.hasOption(SRC_DATASOURCE_ARG)) {
			out.setSrcDataSourceName(in.getOptionValue(SRC_DATASOURCE_ARG));
		}

		if (in.hasOption(SRC_SCHEMA_ARG)) {
			out.setSrcSchemaName(in.getOptionValue(SRC_SCHEMA_ARG));
		}

		if (in.hasOption(SRC_TABLE_ARG)) {
			out.setSrcTableName(in.getOptionValue(SRC_TABLE_ARG));
		}

		if (in.hasOption(COLUMNS_ARG)) {
			out.setColumns(in.getOptionValue(COLUMNS_ARG));
		}

		if (in.hasOption(TGT_DATASOURCE_ARG)) {
			out.setTgtDataSourceName(in.getOptionValue(TGT_DATASOURCE_ARG));
		}

		if (in.hasOption(TGT_SEHCMA_ARG)) {
			out.setTgtSchemaName(in.getOptionValue(TGT_SEHCMA_ARG));
		}

		if (in.hasOption(TGT_TABLE_ARG)) {
			out.setTgtTableName(in.getOptionValue(TGT_TABLE_ARG));
		}

		if (in.hasOption(PARTITION_COLUMN_NAME_ARG)) {
			out.setPartitionColumnName(in.getOptionValue(PARTITION_COLUMN_NAME_ARG));
		}

		if (in.hasOption(PARTITION_COLUMN_VALUE_ARG)) {
			out.setPartitionColumnValue(in.getOptionValue(PARTITION_COLUMN_VALUE_ARG));
		}

		if (in.hasOption(WHERE_ARG)) {
			out.setWhere(in.getOptionValue(WHERE_ARG));
		}

		if (in.hasOption(QUEUE_SIZE_ARG)) {
			out.setQueueSize(Integer.valueOf(in.getOptionValue(QUEUE_SIZE_ARG)));
		}
		

		if (in.hasOption(PARTITION_FIELD_ARG)) {
			out.setPartitionColumnValue(in.getOptionValue(PARTITION_FIELD_ARG));
		}

		if (in.hasOption(SPLIT_COLUMN_ARG)) {
			out.setSplitByColumn(in.getOptionValue(SPLIT_COLUMN_ARG));
		}

		if (in.hasOption(EXCLUDE_COLUMNS_ARG)) {
			out.setExcludeColumns(in.getOptionValue(EXCLUDE_COLUMNS_ARG));
		}

		if (in.hasOption(DIRECT_ARG)) {
			out.setDirect(true);
		}

		if (in.hasOption(SYNC_MODE_ARG)) {

			String syncMode = in.getOptionValue(SYNC_MODE_ARG);
			if (StringUtils.isNotBlank(syncMode)) {
				try {
					out.setSyncMode(SyncroModeEnum.valueOf(syncMode));
				} catch (Exception ex) {
					LOGGER.error("sync mode is null", ex);
				}

			}

		}

		/**
		 * 字段分隔符
		 */
		if (in.hasOption(COLUMN_DIVIDE)) {
			out.setColumnDivide(in.getOptionValue(COLUMN_DIVIDE));
			LOGGER.info("input column-divide is :'" + in.getOptionValue(COLUMN_DIVIDE)+"'");
		}else{
			out.setColumnDivide(HiveDivideConstant.COLUMN_DIVIDE);
			LOGGER.info("use default column-divide is :'" + in.getOptionValue(COLUMN_DIVIDE) +"'");
		}
		
	}

	/**
	 * 初始化同步工具
	 */
	public static SyncTool getTool(String toolName) {
		Class<? extends SyncTool> cls = TOOLS.get(toolName);
		try {
			if (null != cls) {
				SyncTool tool = cls.newInstance(); // 初始化同步工具实例
				tool.setToolName(toolName);
				return tool;
			}
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			return null;
		}

		return null;
	}

	public void printHelp(ToolOptions opts) {
		System.out.println("usage: sync " + getToolName() + " [GENERIC-ARGS] [TOOL-ARGS]");
		System.out.println("");

		opts.printHelp();

		System.out.println("");
		ToolRunner.printGenericCommandUsage(System.out);
	}

	/**
	 * 执行任务
	 * 
	 * @param args
	 * @return
	 */
	public static int sync(String[] args, SyncTool tool) {
		try {
			SyncOptionsDto options = new SyncOptionsDto();
			options = tool.parseArguments(args, options); // 解析参数
			LOGGER.info("Data sync options is : " + options);
			return tool.run(options);
		} catch (Exception e) {
			LOGGER.error("Failed to sync data: ", e);
            return 1;
        }
	}
	
	public abstract int run(SyncOptionsDto options) throws Exception;
}

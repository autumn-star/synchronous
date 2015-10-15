package com.synchro.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.common.constant.SyncConstant;
import com.synchro.common.constant.HiveDivideConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA. User: xingxing.duan Date: 15-05-10 Time: 下午3:24
 * hdfs工具类
 */
public class HdfsUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(HdfsUtils.class);

	private final static Joiner joiner = Joiner.on("\001").useForNull("\\N");

	// initialization
	static Configuration conf = new Configuration();
	public static FileSystem hdfs;

	static {
		String path = SyncConstant.HADOOP_PATH;
		conf.addResource(new Path(path + "core-site.xml"));
		try {
			hdfs = FileSystem.get(conf);
		} catch (IOException e) {
			LOGGER.error("获取FileSystem异常", e);
		}
	}

	// create a direction
	public static void createDir(String dir) throws IOException {
		Path path = new Path(dir);
		if (!HdfsUtils.hdfs.exists(path)) {
			HdfsUtils.hdfs.mkdirs(path);
		} else {
			boolean del = HdfsUtils.hdfs.delete(path, true);
			if (del) {
				HdfsUtils.hdfs.mkdirs(path);
			}
		}
		LOGGER.info("new dir \t" + conf.get("fs.default.name") + dir);
	}

	// copy from local file to HDFS file
	public void copyFile(String localSrc, String hdfsDst) throws IOException {
		Path src = new Path(localSrc);
		Path dst = new Path(hdfsDst);
		hdfs.copyFromLocalFile(src, dst);

		// list all the files in the current direction
		FileStatus files[] = hdfs.listStatus(dst);
		LOGGER.info("Upload to \t" + conf.get("fs.default.name") + hdfsDst);
		for (FileStatus file : files) {
			System.out.println(file.getPath());
		}
	}

	// create a new file
	public static void createFile(String fileName, String fileContent) throws IOException {
		Path dst = new Path(fileName);
		byte[] bytes = fileContent.getBytes();
		FSDataOutputStream output = hdfs.create(dst);
		output.write(bytes);
		LOGGER.info("new file \t" + conf.get("fs.default.name") + fileName);
	}

	// list all files
	public void listFiles(String dirName) throws IOException {
		Path f = new Path(dirName);
		FileStatus[] status = hdfs.listStatus(f);
		System.out.println(dirName + " has all files:");
		for (int i = 0; i < status.length; i++) {
			LOGGER.info(status[i].getPath().toString());
		}
	}

	// judge a file existed? and delete it!
	public void deleteFile(String fileName) throws IOException {
		Path f = new Path(fileName);
		boolean isExists = hdfs.exists(f);
		if (isExists) { // if exists, delete
			boolean isDel = hdfs.delete(f, true);
			LOGGER.info(fileName + "  delete? \t" + isDel);
		} else {
			LOGGER.info(fileName + "  exist? \t" + isExists);
		}
	}

	/**
	 * 生成csv文件
	 *
	 * @param output
	 *            目标文件流
	 * @param columns
	 *            字段
	 * @param set
	 *            游标
	 * @throws Exception
	 */
	public static void makeCSVContent(FSDataOutputStream output, List<ColumnMetaData> columns, SqlRowSet set) throws Exception {
		long t0 = System.currentTimeMillis();
		int lineNum = 0;
		StringBuilder sb = new StringBuilder();
		try {
			while (set.next()) {
				lineNum++;
				List<Object> rowList = Lists.newArrayList();
				for (ColumnMetaData column : columns) {
					Object value = set.getObject(column.getName());
					if (value == null) {
						rowList.add(value);
					} else {
						rowList.add(value.toString().replaceAll(HiveDivideConstant.LINE_DIVIDE, "").replaceAll(HiveDivideConstant.COLUMN_DIVIDE, "")
								.replaceAll(HiveDivideConstant.HIVE_DIVIDE, ""));
					}
				}
				sb.append(joiner.join(rowList)).append("\n");
				// 每5000行提交一次
				if (lineNum % 10000 == 0) {
					byte[] buffer = sb.toString().getBytes("utf-8");
					LOGGER.info("大小 {}", buffer.length);
					output.write(buffer);
					output.flush();
					sb.delete(0, sb.length());
				}

			}
			if (sb.length() > 0) {
				output.write(sb.toString().getBytes("utf-8"));
			}
			output.flush();

		} catch (Exception ex) {
			LOGGER.error("文件创建失败！", ex);
			throw ex;
		}
		long t1 = System.currentTimeMillis();
		LOGGER.info("hdfs 生成[{}]行文件耗时 {} 秒", lineNum, (t1 - t0) / 1000);
	}
}

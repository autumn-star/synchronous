package com.synchro.service.copy;

import com.synchro.common.constant.HiveDivideConstant;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 执行shell类
 * CreateTime 2015-08-09
 * Refactored by Qinghe on 2017-02-13
 * @author liqiu
 */
public class CopyCommandExecuter {
    private static final int SLEEP_TIME = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyCommandExecuter.class);

    /**
     * 执行shell命令
     *
     * @param copyCommand
     * @param columnMetaDatas
     * @return
     */
    public static void execute(String copyCommand, LinkedBlockingQueue<RowData> queue, List<ColumnMetaData> columnMetaDatas) throws Exception {
        Process pid;
        String[] cmd  = {"/bin/sh", "-c", copyCommand};;

        pid = startProcess(cmd);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pid.getInputStream()));

        String line;
        try {
            while ((line = bufferedReader.readLine()) != null || pid.isAlive()) {
                // if process is alive but no output, sleep 100ms to save cpu
                if (line == null) {
                    Thread.sleep(SLEEP_TIME);
                    continue;
                }
                // skip empty lines
                if (line == "") {
                    continue;
                }
                String[] columnObjects = line.split(HiveDivideConstant.COPY_COLUMN_DIVIDE.toString(), -1);

                LOGGER.info("length of columnObjects is: " + columnObjects.length);
                if (columnObjects.length != columnMetaDatas.size()) {
                    LOGGER.error(" 待同步的表有特殊字符，不能使用copy, 分隔数量：" + columnObjects.length + ";字段数量" + columnMetaDatas.size() + "; 内容: " + line);
                    throw new RuntimeException("待同步的表有特殊字符，不能使用copy " + line);
                }
                RowData rowData = new RowData(columnObjects);
                queue.put(rowData);
            }
            int exitStatus = pid.waitFor();
            LOGGER.info("Exit status of command is :" + exitStatus);
            if (exitStatus != 0) {
                throw new RuntimeException("Failed to execute command " + copyCommand);
            }
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            //from the doc of java Process, it seems that the subprocess has no streams by its own, it shares
            //the stdin,stdout,stderr with the parent process, so we don't need to close them explicitly
        }
    }

    private static Process startProcess(String[] cmd) throws Exception{
        try {
            // start another process to run command
            return Runtime.getRuntime().exec(cmd);
        } catch (Exception e) {
            LOGGER.error("Failed to start process to run shell command: ", e);
            throw e;
        }
    }
}

package com.synchro.service.copy;

import com.lowagie.text.Row;
import com.synchro.common.constant.HiveDivideConstant;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
    private static String SPLIT_PATTERN_PREFIX = "(?<!\\\\)";
    private static String ESCAPE_COLUMN_DIVIDE = "\\\\" + HiveDivideConstant.COPY_COLUMN_DIVIDE;

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
            while ((line = bufferedReader.readLine()) != null) {
                // if process is alive but no output, sleep 100ms to save cpu
                if (line == null) {
                    Thread.sleep(SLEEP_TIME);
                    continue;
                }
                assert(line.length() > 0);

                queue.put(parseLine(line, columnMetaDatas.size()));
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
            LOGGER.info("Running command " + cmd[cmd.length - 1]);
            return Runtime.getRuntime().exec(cmd);
        } catch (Exception e) {
            LOGGER.error("Failed to start process to run shell command: ", e);
            throw e;
        }
    }


    private static RowData parseLine(String line, int columnCount) {
        // To improve performance, we try to split line data using the column delimiter first. If it fails, there may
        // be special characters in column fields, then we use SPLIT_PATTERN_PREFIX to split line data
        String[] columnObjects = line.split(HiveDivideConstant.COPY_COLUMN_DIVIDE, -1);

        if (columnObjects.length != columnCount) {
            columnObjects = line.split(SPLIT_PATTERN_PREFIX + HiveDivideConstant.COPY_COLUMN_DIVIDE);
            if (columnObjects.length != columnCount) {
                String errMsg = "Failed to parse line: " + line;
                LOGGER.error(errMsg);
                throw new RuntimeException(errMsg);
            }
            List<String> result = new ArrayList<>();
            for (String s: columnObjects) {
                // Replace the escaped column divide
                result.add(s.replace(ESCAPE_COLUMN_DIVIDE, HiveDivideConstant.COPY_COLUMN_DIVIDE));
            }
            result.toArray(columnObjects);
        }
        return new RowData(columnObjects);
    }
}

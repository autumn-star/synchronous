package com.synchro.service.copy;

import com.synchro.common.constant.HiveDivideConstant;
import com.synchro.common.constant.SyncConstant;
import com.synchro.dal.metadata.ColumnMetaData;
import com.synchro.dal.metadata.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 执行shell类
 * CreateTime 2015-08-09
 *
 * @author liqiu
 */
public class ShellExecuter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShellExecuter.class);

    /**
     * 执行shell命令
     *
     * @param shellPath
     * @param columnMetaDatas
     * @return
     */
    public static int execute(String shellPath, LinkedBlockingQueue<RowData> queue, List<ColumnMetaData> columnMetaDatas) {

        int success = -1;
        Process pid = null;
        String[] cmd;

        try {
            cmd = new String[]{"/bin/sh", "-c", shellPath};
            // 执行Shell命令
            pid = Runtime.getRuntime().exec(cmd);
            if (pid != null) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(pid.getInputStream()), SyncConstant.SHELL_STREAM_BUFFER_SIZE);
                try {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        // LOGGER.info(String.format("shell info output [%s]", line));
                        String[] columnObjects = line.split(HiveDivideConstant.COPY_COLUMN_DIVIDE.toString(), -1);
                        if (columnObjects.length != columnMetaDatas.size()) {
                            LOGGER.error(" 待同步的表有特殊字符，不能使用copy [{}] ", line);
                            throw new RuntimeException("待同步的表有特殊字符，不能使用copy " + line);
                        }
                        RowData rowData = new RowData(line.split(HiveDivideConstant.COPY_COLUMN_DIVIDE.toString(), -1));
                        queue.put(rowData);
                    }
                } catch (Exception ioe) {
                    LOGGER.error(" execute shell error", ioe);
                } finally {
                    try {
                        if (bufferedReader != null) {
                            bufferedReader.close();
                        }
                    } catch (Exception e) {
                        LOGGER.error("execute shell, get system.out error", e);
                    }
                }
                success = pid.waitFor();
                if (success != 0) {
                    LOGGER.error("execute shell error ");
                }
            } else {
                LOGGER.error("there is not pid ");
            }
        } catch (Exception ioe) {
            LOGGER.error("execute shell error", ioe);
        } finally {
            if (null != pid) {
                try {
                    //关闭错误输出流
                    pid.getErrorStream().close();
                } catch (IOException e) {
                    LOGGER.error("close error stream of process fail. ", e);
                } finally {
                    try {
                        //关闭标准输入流
                        pid.getInputStream().close();
                    } catch (IOException e) {
                        LOGGER.error("close input stream of process fail.", e);
                    } finally {
                        try {
                            pid.getOutputStream().close();
                        } catch (IOException e) {
                            LOGGER.error(String.format("close output stream of process fail.", e));
                        }
                    }
                }
            }
        }

        return success;
    }

    public static void main(String[] args) {
        LinkedBlockingQueue<RowData> queue = new LinkedBlockingQueue<RowData>();
        try {
            String host = "l-tdata2.tkt.cn6.ShellExecuter.com";
            String user = "tkt_data_dev";
            String execCopy = "/Library/PostgreSQL/9.3/bin/psql -h " + host + " -U " + user + " log_analysis -c \"COPY (select * from mirror.b2c_product_ticket_date limit 3) TO STDOUT DELIMITER '	'\""; // 执行copy命令
            Runtime.getRuntime().exec(execCopy);
            List<ColumnMetaData> columnMetaDatas = new ArrayList<ColumnMetaData>();
            ShellExecuter.execute(execCopy, queue, columnMetaDatas);
            RowData t;
            do {
                t = queue.poll();
                System.out.println(t);
            }
            while (t != null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

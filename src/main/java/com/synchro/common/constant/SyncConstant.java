package com.synchro.common.constant;

/**
 * 常量定义
 *
 * @author liqiu
 * @createtime 2015-05-30
 */
public interface SyncConstant {

    int THREAD_POOL_SIZE = 5; // 线程池大小
    int INSERT_SIZE = 30000; // 缓存多少insert语句
    int QUEUE_SIZE = THREAD_POOL_SIZE * INSERT_SIZE; // 缓存队列的长度
    int LOGGER_SIZE = 100 * 10000; // 多少条打印一次日志

    int PG_TO_HIVE_THREAD_POOL_SIZE = 5; // 线程池大小
    int PG_TO_HIVE_LINE_SIZE = 100000; // 缓存多少行语句
    int PG_TO_HIVE_QUEUE_SIZE = THREAD_POOL_SIZE * INSERT_SIZE; // 缓存队列的长度
    int PG_TO_HIVE_THRESHOLD = 200000; // 阀值
    int PG_TO_HIVE_SELECT_FAILD_SLEEP_TIME = 60 * 1000; // sql失败重试时间


    int JDBC_CONNECTIONS_INITIAL_SIZE = 1; // 初始化连接数量
    int JDBC_CONNECTIONS_MAX_ACTIVE = 5; // 最大链接数量
    int JDBC_CONNECTIONS_MIN_IDLE = 1; // 最小空闲链接
    int JDBC_CONNECTIONS_MAX_IDLE = 5; // 最大空闲链接

    int SHELL_STREAM_BUFFER_SIZE = 1024;

    String HADOOP_PATH = "/home/q/hadoop/hadoop-2.2.0/etc/hadoop/"; // HADOOP路径
}
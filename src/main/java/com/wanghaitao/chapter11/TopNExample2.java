package com.wanghaitao.chapter11;

import com.wanghaitao.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector = 'filesystem', " +
                " 'path' = 'input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createDDL);


        // 窗口Top N，统计一段时间内的前两名活跃用户
        String subQuery = "SELECT user, COUNT(url) AS cnt, window_start, window_end " +
                "FROM TABLE (" +
                " TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND)" +
                ")" +
                "GROUP BY user, window_start, window_end";

        Table windowTopNResultTable = tableEnv.sqlQuery("SELECT user, cnt, row_num " +
                "FROM (" +
                "   SELECT *, ROW_NUMBER() OVER (" +
                "       PARTITION BY window_start, window_end " +
                "       ORDER BY cnt DESC" +
                "     ) AS row_num " +
                "   FROM ( " + subQuery + ")" +
                ") WHERE row_num <= 2");
        tableEnv.toDataStream(windowTopNResultTable).print("window TopN");
        env.execute();
    }
}

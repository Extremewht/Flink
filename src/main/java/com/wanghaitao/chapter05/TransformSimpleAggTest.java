package com.wanghaitao.chapter05;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary","./home",1000L),
                new Event("Bob","./cart",2000L),
                new Event("Alice","./prod?id=100",3000L),
                new Event("Bob","./prod?id=1",3300L),
                new Event("Bob","./home",3500L),
                new Event("Bob","./prod?id=1",3800L),
                new Event("Bob","./prod?id=1",4200L)
        );
        // 按键分区之后进行聚合，提取当前用户最后一次访问的数据
        //1.传入匿名类
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp")
                        .print("max: ");

        //2.Lambda表达式
        stream.keyBy(data -> data.user)
                        .maxBy("timestamp")
                                .print("maxBy: ");

        env.execute();
    }
}

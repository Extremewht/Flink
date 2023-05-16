package com.wanghaitao.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

class TransformFlapMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary","./home",1000L),
                new Event("Bob","./cart",2000L),
                new Event("Alice","./prod?id=100",3000L));
        //1.实现FlatMapFunction
        stream.flatMap(new MyFlapMap()).print("1");

        //2.传入匿名类实现FlatMapFunction接口
        stream.flatMap(new FlatMapFunction<Event, Object>() {
            @Override
            public void flatMap(Event event, Collector<Object> collector) throws Exception {
                collector.collect(event.user);
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            }
        }).print("2");

        //3.传入一个Lambda表达式
        stream.flatMap((Event event, Collector<String> out) -> {
            if (event.user.equals("Mary"))
                out.collect(event.url);
            else if (event.user.equals("Bob")){
                out.collect(event.user);
                out.collect(event.url);
                out.collect(event.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {})
                .print("3");

        env.execute();
    }

    public static class MyFlapMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}

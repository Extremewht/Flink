package com.wanghaitao.chapter06;

import com.wanghaitao.chapter05.ClickSource;
import com.wanghaitao.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        // 从元素中读取数据
//        SingleOutputStreamOperator<Event> stream = env.fromElements(
//                new Event("Mary","./home",1000L),
//                new Event("Bob","./cart",2000L),
//                new Event("Bob","./cart",2000L),
//                new Event("Alice","./prod?id=100",3000L),
//                new Event("Bob","./prod?id=1",3300L),
//                new Event("Bob","./home",3500L),
//                new Event("Bob","./prod?id=1",3800L),
//                new Event("Bob","./prod?id=1",4200L))
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.map(new MapFunction<Event, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.user, 1L);
            }
        })
            .keyBy(data -> data.f0)
                //  .countWindow(10,2) //滑动计数窗口
                //  .window(EventTimeSessionWindows.withGap(Time.seconds(2))) //会话事件窗口
                //  .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5))) //滑动事件窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) //滚动事件时间窗口
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1+value2.f1);
                    }
                })
                        .print();
        env.execute();
    }
}

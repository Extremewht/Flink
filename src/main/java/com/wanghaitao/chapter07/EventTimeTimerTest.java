package com.wanghaitao.chapter07;

import com.wanghaitao.chapter05.ClickSource;
import com.wanghaitao.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long l) {
                                return element.timestamp;
                            }
                        })
                );
        //事件时间定时器
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        //获取当前时间戳
                        Long currTs = ctx.timestamp();
                        //收集数据到控制台
                        out.collect(ctx.getCurrentKey()+" 数据到达，到达时间： " + new Timestamp(currTs)+ "watermark: " + ctx.timerService().currentWatermark());
                        //注册一个10s的定时器
                        ctx.timerService().registerProcessingTimeTimer(currTs+ 10000L);
                    }
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间： " + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }

    //自定义测试数据源
    public static class CustomSource implements SourceFunction<Event>{
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            // 直接发出测试数据
            ctx.collect(new Event("Mary","./home",1000L));

            Thread.sleep(5000L);

            ctx.collect(new Event("Alice","./home",11000L));
            Thread.sleep(5000L);

            ctx.collect(new Event("Bob","./home",11001L));
            Thread.sleep(5000L);


        }

        @Override
        public void cancel() {

        }
    }
}

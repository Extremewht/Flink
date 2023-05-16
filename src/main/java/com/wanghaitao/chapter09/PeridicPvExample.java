package com.wanghaitao.chapter09;

import com.wanghaitao.chapter05.ClickSource;
import com.wanghaitao.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeridicPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long timestamp) {
                                return event.timestamp;
                            }
                        }));
        stream.print("input");

        // 统计每个用户的pv
        stream.keyBy(data -> data.user)
                        .process(new PeridicPvResult())
                                .print();
        env.execute();
    }

    //实现自定义的KeyProcessFunction
    public static class PeridicPvResult extends KeyedProcessFunction<String,Event,String>{
        //定义状态，保存当前pv统计值,以及有么有定时器
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，就更新对应的count值
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);

            //注册定时器.
            if (timerTsState.value() == null){
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10000L);
                timerTsState.update(value.timestamp + 10000L);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出一次统计结果
            out.collect(ctx.getCurrentKey() + "的pv: " + countState.value());
            //清空状态
            timerTsState.clear();
        }
    }
}

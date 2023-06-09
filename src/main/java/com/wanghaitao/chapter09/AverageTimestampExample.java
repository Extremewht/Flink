package com.wanghaitao.chapter09;

import com.wanghaitao.chapter05.ClickSource;
import com.wanghaitao.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class AverageTimestampExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );
        stream.print("input");

        //自定义实现平均时间戳的统计
        stream.keyBy(data ->data.user)
                        .flatMap(new AvgTsResult(5L))
                                .print();

        env.execute();
    }

    //实现自定义的RichFlatmapFunction
    public static class AvgTsResult extends RichFlatMapFunction<Event,String>{

        private Long count;

        public AvgTsResult(Long count) {
            this.count = count;
        }

        //定义一个聚合状态，用来保存平均时间戳
        AggregatingState<Event,Long> avgTsAggState;

        //定义一个值状态保存用户访问的次数
        ValueState<Long> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            avgTsAggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long,Long>, Long>(
                    "avg-ts",
                    new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L,0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + value.timestamp, accumulator.f1 + 1);
                        }

                        @Override
                        public Long getResult(Tuple2<Long, Long> longLongTuple2) {
                            return createAccumulator().f0 / createAccumulator().f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                            return null;
                        }
                    },
                    Types.TUPLE(Types.LONG, Types.LONG)
            ));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            //每来一条数据，currcount + 1
            Long currcount = countState.value();
            if (currcount == null)
                currcount = 1L;
            else
                currcount ++;

            //更新状态
            countState.update(currcount);
            avgTsAggState.add(value);

            //如果达到count次数就输出结果
            if (currcount.equals(count)){
                out.collect(value.user + "过去" + count + "次访问平均时间戳为： " + avgTsAggState.get());
                //清理状态
                countState.clear();
                avgTsAggState.clear();
            }
        }
    }
}

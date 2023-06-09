package com.wanghaitao.chapter06;

import com.wanghaitao.chapter05.ClickSource;
import com.wanghaitao.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);

//        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
          SingleOutputStreamOperator<Event> stream = env.socketTextStream("hadoop102",7777)
                  .map(new MapFunction<String, Event>() {
                      @Override
                      public Event map(String s) throws Exception {
                          String[] fields = s.split(" ");
                          return new Event(fields[0].trim(),fields[1].trim(),Long.valueOf(fields[2].trim()));
                      }
                  })
                  .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.print("input");

        //定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("lates"){};

        // 统计每个URL的访问量
        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) //将窗口允许延迟1分钟
                .sideOutputLateData(late)  //设置测输出流，传入一个输出标签，将迟到的数据放入侧输出流中
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        result.print("result");

        result.getSideOutput(late).print("late");//获取侧输出流

        env.execute();
    }
}

package com.wanghaitao.chapter05;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary","./home",1000L),
                new Event("Bob","./cart",2000L),
                new Event("Bob","./cart",2000L),
                new Event("Alice","./prod?id=100",3000L),
                new Event("Bob","./prod?id=1",3300L),
                new Event("Bob","./home",3500L),
                new Event("Bob","./prod?id=1",3800L),
                new Event("Bob","./prod?id=1",4200L)
        );
        //1.随机分区shuffle
        // stream.shuffle().print().setParallelism(4);

        //2.轮询分区
        //stream.rebalance().print().setParallelism(4);

        //3.重缩放分区rescale
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1; i <= 8; i++) {
                    //将奇偶数分别发送到0号和1号分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2)
              /*          .rescale()
                                .print()*/
                                        .setParallelism(4);

        //4.广播
        //stream.broadcast().print().setParallelism(4);

        //5.全局分区
        //stream.global().print().setParallelism(4);
        
        //6.自定义重分区
        env.fromElements(1,2,3,4,5,6,7,8)
                        .partitionCustom(new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key % 2 ;
                            }
                        }, new KeySelector<Integer, Integer>() {
                            @Override
                            public Integer getKey(Integer integer) throws Exception {
                                return integer;
                            }
                        })
                                .print().setParallelism(4);
        env.execute();
    }
}

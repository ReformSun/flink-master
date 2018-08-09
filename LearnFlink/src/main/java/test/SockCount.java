package test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SockCount {
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.addSource(new SunSocketTextStreamFunction(9000,"localhost"));
        DataStream<SunWordWithCount> windowCounts = text.flatMap(new SunFlatMapFunction()).keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new SunReduceFunction());


        windowCounts.print().setParallelism(1);

        try {
            env.execute("Socket Window WordCount");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

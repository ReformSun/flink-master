package test;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SocketTest {
    public static void main(String[] args) {

        ArrayList list = new ArrayList(){{
            add("key1");
            add("key2");
        }};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.addSource(new SunSocketTextStreamFunction(9000,"localhost"));


        DataStream<SunWordWithKey> windowSplit = text.flatMap(new SunFlatMapFunction_2("/",list));

        windowSplit.print().setParallelism(1);

//        WindowedStream windowedStream = windowSplit.keyBy("value").window(TumblingProcessingTimeWindows.of(Time.seconds(1)));

//        .reduce(new SunReduceFunction_2());
//        windowSplit.addSink(new PrintSinkFunction<SunWordWithKey>())

//        windowSplit.print().setParallelism(1);

        try {
            env.execute("Socket Window WordSplit");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

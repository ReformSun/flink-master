package socket;

import com.test.map.Test;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

public class TestThread extends Thread{

    private DataStream dataStream;
    private StreamExecutionEnvironment streamExecutionEnvironment;

    public TestThread(DataStream dataStream,StreamExecutionEnvironment env) {
        this.dataStream = dataStream;
        this.streamExecutionEnvironment = env;
    }

    @Override
    public void run() {
        Socket socket = new Socket();
        try{
            socket.connect(new InetSocketAddress("localhost",9999),0);
        }catch (IOException e){
            e.printStackTrace();
        }

//        AppendStreamTableSink appendStreamTableSink = new AppendStreamTableSink();

        try{
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String temp;
            while ((temp = bufferedReader.readLine()) != null) {
                DataStream<String> dataStream2 = dataStream.map(new Test());
                dataStream2.print().setParallelism(1);
                this.streamExecutionEnvironment.execute("Socket Window WordSplit");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        super.run();

    }
}

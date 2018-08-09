package test;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SunSocketTextStreamFunction implements SourceFunction<String> {

    private final int port;
    private final String hostName;

    public SunSocketTextStreamFunction(int port, String hostName) {
        this.port = port;
        this.hostName = hostName;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Socket socket = new Socket();
        try{
            socket.connect(new InetSocketAddress(hostName,port),0);
        }catch (IOException e){
            e.printStackTrace();
        }

        try{
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String temp;
            while ((temp = bufferedReader.readLine()) != null) {
                System.out.println(temp);
                sourceContext.collect(temp);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {

    }
}

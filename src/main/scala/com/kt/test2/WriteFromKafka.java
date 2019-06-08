package com.kt.test2;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class WriteFromKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties pro = new Properties();
        pro.put("bootstrap.servers","192.168.22.100:9092");
        DataStreamSource<String> streamSource = env.addSource(new SimpleStringGenerator());
        streamSource.addSink(new FlinkKafkaProducer010<String>("flink-test", new SimpleStringSchema(), pro));
        env.execute();



    }

    private static class SimpleStringGenerator implements SourceFunction<String>{
        private boolean running = true;
        private long i = 0;


        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (running){
                sourceContext.collect("Flink" + (i++));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }




}

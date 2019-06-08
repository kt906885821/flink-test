package com.kt.test2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class ReadFromKafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(5000);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties pro = new Properties();

        pro.put("bootstrap.server","192.168.22.100:9092");
        pro.put("group.id","flink_consumer");
        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("flink-test", new SimpleStringSchema(), pro);
        DataStreamSource<String> streamSource = environment.addSource(kafkaConsumer);
        streamSource.setParallelism(3);
        kafkaConsumer.setStartFromGroupOffsets();

        streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "Stream Value" + s;
            }
        }).print();

        environment.execute();
    }



}

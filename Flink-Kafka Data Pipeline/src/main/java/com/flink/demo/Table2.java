package com.flink.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.apache.flink.table.sources.tsextractors.TimestampExtractor;
import org.apache.flink.types.Row;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;


public class Table2 {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        Properties sinkProperties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink_consumer");
        sinkProperties.setProperty("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>("INPUT", new SimpleStringSchema(), properties);

        DataStream<Tuple9<Integer, String, String, String, String, Integer, Long, Float, DateTime>> stream = fsEnv.addSource(consumer)
                .filter(new FilterFunction<String>() {
                    public boolean filter(String line) {
                        if (line.contains("Customer_id")) return false;
                        else return true;
                    }
                }).map(new MapFunction<String, Tuple9<Integer , String, String, String, String, Integer, Long, Float, DateTime>>() {
                    @Override
                    public Tuple9<Integer , String, String, String, String, Integer, Long, Float, DateTime> map(String str) throws Exception {
                        String[] temp = str.split(",");
                        return new Tuple9<>(
                                Integer.parseInt(temp[0]),
                                String.valueOf(temp[1]),
                                String.valueOf(temp[2]),
                                String.valueOf(temp[3]),
                                String.valueOf(temp[4]),
                                Integer.parseInt(temp[5]),
                                Long.parseLong(temp[6]),
                                Float.parseFloat(temp[7]),
                                DateTimeFormat.forPattern("dd/MM/yyyy").parseDateTime(temp[8]));
                    }
                }).filter(new FilterFunction<Tuple9<Integer, String, String, String, String, Integer, Long, Float, DateTime>>() {
                    @Override
                    public boolean filter(Tuple9<Integer, String, String, String, String, Integer,
                            Long, Float, DateTime> link) throws Exception {
                        if (link.f3.equals("F")) {
                            return true;
                        }
                        else {
                            return false;
                        }
                    }
                });

        stream.map(new MapFunction<Tuple9<Integer, String, String, String, String, Integer, Long, Float, DateTime>, String>() {
            @Override
            public String map(Tuple9<Integer, String, String, String, String, Integer, Long, Float, DateTime> link) throws Exception {
                return link.f0.toString() + "," + link.f1.toString() + "," + link.f2.toString()  + "," + link.f3.toString() + "," + link.f4.toString() + "," +
                        link.f5.toString() + "," + link.f6.toString() + "," + link.f7.toString() + "," + link.f8.toString();
            }
        }).addSink(new FlinkKafkaProducer09<>("SINK", new SimpleStringSchema(), sinkProperties));

        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

        FlinkKafkaConsumer09<String> consumer1 = new FlinkKafkaConsumer09<>("SINK", new SimpleStringSchema(), properties);

        DataStream<Tuple9<Integer, String, String, String, String, Integer, Long, Float, String>> stream1 = fsEnv.addSource(consumer1)
                .map(new MapFunction<String, Tuple9<Integer , String, String, String, String, Integer, Long, Float, String>>() {
                    @Override
                    public Tuple9<Integer , String, String, String, String, Integer, Long, Float, String> map(String str1) throws Exception {
                        String[] jkr = str1.split(",");
                        return new Tuple9<>(
                                Integer.parseInt(jkr[0]),
                                String.valueOf(jkr[1]),
                                String.valueOf(jkr[2]),
                                String.valueOf(jkr[3]),
                                String.valueOf(jkr[4]),
                                Integer.parseInt(jkr[5]),
                                Long.parseLong(jkr[6]),
                                Float.parseFloat(jkr[7]),
                                String.valueOf(jkr[8]));
                    }
                });


        Table table = fsTableEnv.fromDataStream(stream1).as("ID, NAME, SURNAME, GENDER, CITY, AGE, ACCOUNT_ID, TRN_AMOUNT, TRN_DATE");
        fsTableEnv.registerTable("MyTable",table);
        String sqlQuery = "SELECT ID, COUNT(TRN_AMOUNT) as d FROM MyTable GROUP BY ID ";

        Table counts = fsTableEnv.sqlQuery(sqlQuery);

        DataStream<Tuple2<Boolean, Row>> result = fsTableEnv.toRetractStream(counts, Row.class);

        result.map(new MapFunction<Tuple2<Boolean, Row>, String>() {
            @Override
            public String map(Tuple2<Boolean, Row> link1) throws Exception {
                return  link1.f1.toString();
            }
        }).addSink(new FlinkKafkaProducer09<String>("SINK1", new SimpleStringSchema(), sinkProperties));


        FlinkKafkaConsumer09<String> consumer2 = new FlinkKafkaConsumer09<>("SINK1", new SimpleStringSchema(), properties);

        DataStreamSink<Tuple2<Integer, Integer>> stream2 = fsEnv.addSource(consumer2)
                .map(new MapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(String str2) throws Exception {
                        String[] jkd = str2.split(",");
                        return new Tuple2<>(
                                Integer.parseInt(jkd[0]),
                                Integer.parseInt(jkd[1]));
                    }
                }).keyBy(0).timeWindowAll(Time.seconds(3)).max(1).print();

        fsEnv.execute();


    }
}
package mydemo;

/**
 * Let's see if this will work
 *
 */
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class App {

    public static void main(String[] args) throws Exception {

        JSONParser parser = new JSONParser();
        Object obj = null;
        try {
            obj = parser.parse(new FileReader("/home/geo/Desktop/config.json"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONObject jsonObject = (JSONObject) obj;

        String function = jsonObject.get("Function").toString();
        String source = jsonObject.get("Source").toString();
        String sink = jsonObject.get("Sink").toString();


        switch (function){
            case "Filter":{
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                Properties properties = new Properties();
                Properties sinkProperties = new Properties();
                properties.setProperty("bootstrap.servers", "localhost:9092");
                properties.setProperty("group.id", "flink_consumer");
                sinkProperties.setProperty("bootstrap.servers", "localhost:9092");
                final Integer value = Integer.parseInt(jsonObject.get("Value").toString());
                final String param = jsonObject.get("Param").toString();

                FlinkKafkaConsumer09<String> consumer = new FlinkKafkaConsumer09<>(source, new SimpleStringSchema(), properties);

                DataStream<String[]> stream = env.addSource(consumer).filter(new FilterFunction<String>() {
                    public boolean filter(String line) {
                        if (line.contains("Customer_id")) {
                            return false;
                        } else {
                            return true;}
                    }
                }).map(new MapFunction<String, String[]>() {
                    @Override
                    public String[] map(String value) throws Exception {
                        String [] jkr = value.split(",");
                        return jkr;
                    }
                }).filter(new FilterFunction<String []>() {
                    @Override
                    public boolean filter(String [] jkr) throws Exception {
                        if (jkr[value].equals(param)) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });

                stream.map(new MapFunction<String[], String>() {
                    @Override
                    public String map(String[] jkr) throws Exception {
                        return jkr[0].toString() + "," + jkr[1].toString() + "," +
                                jkr[2].toString() + "," + jkr[3].toString() + "," + jkr[4].toString() + "," + jkr[5].toString();
                    }
                }).addSink(new FlinkKafkaProducer09<String>(sink,new SimpleStringSchema(),sinkProperties));

                env.execute();
                break;
            }
        }
    }
}
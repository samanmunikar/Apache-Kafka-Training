package Producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class SupplierProducer {
    public static void main(String[] args) throws Exception {
        String topicName = "SupplierTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "Producers.SupplierSerializer");

        Producer<String, Supplier> producer = new KafkaProducer<>(props);

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Supplier sp1 = new Supplier(101, "XYZ Pvt Ltd.",df.parse("2020-09-06"));
        Supplier sp2 = new Supplier(102, "ABC Pvt Ltd.",df.parse("2019-10-22"));

        producer.send(new ProducerRecord<String,Supplier>(topicName, "SUP", sp1)).get();
        producer.send(new ProducerRecord<String,Supplier>(topicName, "SUP", sp2)).get();

        producer.close();

        System.out.println("Supplier Completed.");
    }
}

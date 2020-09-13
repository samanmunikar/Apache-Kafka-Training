package Consumers;

import Producers.Supplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class NewSupplierConsumer {
    public static void main(String[] args) throws IOException {
        String topicName = "SupplierTopic";

        Properties props = new Properties();
        InputStream input = null;
        KafkaConsumer<String, Supplier> consumer = null;

        try {
            input = new FileInputStream("src/resources/SupplierConsumer.properties");
            props.load(input);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName));

            while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records){
                    System.out.println("Supplier id= " + String.valueOf(record.value().getSupplierId())
                            + " Supplier  Name = " + record.value().getSupplierName()
                            + " Supplier Start Date = " + record.value().getSupplierStartDate().toString());
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            input.close();
            consumer.close();
        }
    }
}

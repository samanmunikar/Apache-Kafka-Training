package Consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

public class SensorConsumer {
    public static void main(String[] args) throws Exception {
        String topicName = "SensorTopic";
        int recordCount;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        TopicPartition p0 = new TopicPartition(topicName, 0);
        TopicPartition p1 = new TopicPartition(topicName, 1);
        TopicPartition p2 = new TopicPartition(topicName, 2);

        consumer.assign(Arrays.asList(p0,p1,p2));
        System.out.println("Current position p0=" + consumer.position(p0)
                + " p1=" + consumer.position(p1)
                + " p2=" + consumer.position(p2));

        consumer.seek(p0, getOffsetFromDB(p0));
        consumer.seek(p1, getOffsetFromDB(p1));
        consumer.seek(p2, getOffsetFromDB(p2));
        System.out.println("New positions po=" + consumer.position(p0)
                + " p1=" + consumer.position(p1)
                + " p2=" + consumer.position(p2));

        try {
            do {
                ConsumerRecords<String, String> records = consumer.poll(100);
                System.out.println("Record polled " + records.count());
                recordCount = records.count();
                for(ConsumerRecord<String, String> record : records) {
                    saveAndCommit(consumer, record);
                }
            } while (recordCount > 0);
        } catch (Exception e) {
            System.out.println("Sensor Consumer Exception.");
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static long getOffsetFromDB(TopicPartition p) {
        long offset = 0;
        try {
            System.out.println("Topic = " + p.topic() + " Parition = " + p.partition());
            Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Hackafest3.0", "postgres",
            "postgres");
            String sql = "select * from cotiviti.tss_offsets where topic_name='"+p.topic()+"' and partition="+ p.partition();
            Statement statement = con.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if(rs.next()) {
                offset = rs.getInt("offset");
            }
            statement.close();
            con.close();
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }
        return offset;
    }

    public static void saveAndCommit(KafkaConsumer<String, String> consumer, ConsumerRecord record) {
        System.out.println("Topic = " + record.topic() + " Partition = " + record.partition() + " Offset = " + record.offset()
        + " Key = " + record.key() + " Value = " + record.value());
        try {
            Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/Hackafest3.0", "postgres",
                    "postgres");
            String insertQuery = "insert into cotiviti.tss_data values (?,?)";
            PreparedStatement psInsert = con.prepareStatement(insertQuery);
            psInsert.setString(1, (String) record.key());
            psInsert.setString(2, (String) record.value());

            String updateQuery = "update cotiviti.tss_offsets set \"offset\"=? where topic_name=? and partition=?";
            PreparedStatement psUpdate = con.prepareStatement(updateQuery);
            psUpdate.setLong(1, record.offset()+1);
            psUpdate.setString(2, record.topic());
            psUpdate.setInt(3, record.partition());

            psInsert.executeUpdate();
            psUpdate.executeUpdate();

            //con.commit();
            con.close();
        } catch (SQLException e) {
            System.out.println("Exception in save and commit.");
            e.printStackTrace();
        }
    }
}

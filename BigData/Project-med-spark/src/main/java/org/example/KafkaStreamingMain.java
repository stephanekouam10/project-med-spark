package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.medicaments.functions.processor.MedicamentStreamKafkaProcessor;
import org.example.medicaments.functions.processor.MedicamentStreamProcessor;
import org.example.medicaments.functions.receiver.KafkaReceiver;
import org.example.medicaments.functions.receiver.MedicamentReceiver;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class KafkaStreamingMain {
    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Hello world!");

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.spark.master");
        String appName = config.getString("app.spark.appname");

        String inputPath = config.getString("app.path.input");
        String outputPath = config.getString("app.path.output");
        String checkPointPath = config.getString("app.path.checkpoint");
        List<String> topics = config.getStringList("app.kafka.topics");
//        String servers = config.getString("kafka.bootstrap.servers");
//        String groupId = config.getString("kafka.group.id");
//        String autoReset = config.getString("kafka.auto.offset.reset");

        log.info("\ninputPathStr={}\noutputPathStr={}\ncheckPointStr={}", inputPath, outputPath, checkPointPath);
        log.info("\nmasterUrl={}\nappName={}", masterUrl, appName);

        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName(appName);

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());
        log.info("fileSystem got from sparkSession in the main : hdfs.getScheme = {}", hdfs.getScheme());

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(
                checkPointPath,
                () -> {
                    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
                            new Duration(1000 * 10)
                    );
                    javaStreamingContext.checkpoint(checkPointPath);


                    KafkaReceiver receiver = new KafkaReceiver(topics, javaStreamingContext);
                    MedicamentStreamKafkaProcessor streamProcessor = new MedicamentStreamKafkaProcessor(outputPath);

                    receiver.get().foreachRDD(streamProcessor);

                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()
        );

        jsc.start();
        jsc.awaitTermination();

    }
}
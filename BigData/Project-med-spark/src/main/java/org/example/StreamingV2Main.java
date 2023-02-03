package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.medicaments.functions.processor.MedicamentStreamProcessor;
import org.example.medicaments.functions.receiver.MedicamentReceiver;

import java.io.IOException;

@Slf4j
public class StreamingV2Main {
    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Hello world!");

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.spark.master");
        String appName = config.getString("app.spark.appname");

        SparkSession sparkSession = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        String inputPath = config.getString("app.path.input");
        String outputPath = config.getString("app.path.output");
        String checkPointPath = config.getString("app.path.checkpoint");

        log.info("\ninputPathStr={}\noutputPathStr={}\ncheckPointStr={}", inputPath, outputPath, checkPointPath);
        log.info("\nmasterUrl={}\nappName={}", masterUrl, appName);

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
                    return javaStreamingContext;
                },
                sparkSession.sparkContext().hadoopConfiguration()
        );

        MedicamentReceiver receiver = new MedicamentReceiver(inputPath, jsc );
        MedicamentStreamProcessor streamProcessor = new MedicamentStreamProcessor(outputPath);

        receiver.get().foreachRDD(streamProcessor);

        jsc.start();
        jsc.awaitTerminationOrTimeout(1000 * 60 * 5);


    }
}
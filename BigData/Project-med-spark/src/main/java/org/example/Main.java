package org.example;

import com.esotericsoftware.minlog.Log;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.funcs.StatisticAggFunc;
import org.example.medicaments.functions.funcs.StatisticFunc;
import org.example.medicaments.functions.reader.TextFileReader;
import org.example.medicaments.functions.writer.TextFileWriter;

import java.io.IOException;

import static org.apache.spark.sql.functions.count;

@Slf4j
public class Main {
    public static void main(String[] args) throws IOException {

        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.spark.master");
        String appName = config.getString("app.spark.appname");

        SparkSession sparkSession = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());

        String inputPath = config.getString("app.path.input");
        String outputPath = config.getString("app.path.output");

        TextFileReader reader = new TextFileReader(inputPath, hdfs, sparkSession);
        Dataset<Medicament> ds = reader.get();
//        ds.printSchema();
//        ds.show(5, false);


        TextFileWriter writer = new TextFileWriter(outputPath);
        writer.accept(ds);

//        this is implementation function count nb dataset element
        StatisticFunc statisticFunc = new StatisticFunc();
        long nbcount = statisticFunc.apply(ds);
        Log.info("nb count {} = ", String.valueOf(nbcount));

        StatisticAggFunc statisticAggFunc = new StatisticAggFunc();
        Dataset<Row> rowAgg = statisticAggFunc.apply(ds);
        rowAgg.printSchema();
        rowAgg.show(5, false);

        System.out.println("Hello world!");
    }
}
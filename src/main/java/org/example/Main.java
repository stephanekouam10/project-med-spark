package org.example;

import com.esotericsoftware.minlog.Log;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.parser.MedicamentMapper;
import org.example.medicaments.functions.parser.StatisticAggFunc;
import org.example.medicaments.functions.parser.StatisticFunc;
import org.example.medicaments.functions.reader.TextFileReader;
import org.example.medicaments.functions.writer.TextFileWriter;

import static org.apache.spark.sql.functions.count;

public class Main {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("application.conf");
        String masterUrl = config.getString("app.spark.master");
        String appName = config.getString("app.spark.appname");

        SparkSession sparkSession = SparkSession.builder().master(masterUrl).appName(appName).getOrCreate();

        String inputPath = config.getString("app.path.input");
        String outputPath = config.getString("app.path.output");

        TextFileReader reader = new TextFileReader(inputPath, sparkSession);
        Dataset<Row> ds = reader.get();
//        ds.printSchema();
//        ds.show(5, false);

        Dataset<Medicament> cleanDS = new MedicamentMapper().apply(ds);
//        cleanDS.printSchema();
//        cleanDS.show(5, false);

        TextFileWriter writer = new TextFileWriter(outputPath);
        writer.accept(cleanDS);

//        this is implementation function count nb dataset element
        StatisticFunc statisticFunc = new StatisticFunc();
        long nbcount = statisticFunc.apply(cleanDS);
        Log.info("nb count {} = ", String.valueOf(nbcount));

        StatisticAggFunc statisticAggFunc = new StatisticAggFunc();
        Dataset<Row> rowAgg = statisticAggFunc.apply(cleanDS);
        rowAgg.printSchema();
        rowAgg.show(5, false);



//        EPO-Fc                                                        |0





//        Dataset<Row> ds = sparkSession.read().option("delimiter", ";").option("header", "true").csv(inputPath);
//        ds.printSchema();
//        ds.show(5, false);




        //  Dataset<Row> statds = ds.groupBy("Substance").agg(count("Voie").as("nb"));
        // statds.write().partitionBy("name").parquet(outputPath);

        System.out.println("Hello world!");
    }
}
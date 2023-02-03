package org.example.medicaments.functions.processor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.writer.TextFileWriter;

@Slf4j
@RequiredArgsConstructor
public class MedicamentStreamKafkaProcessor implements VoidFunction<JavaRDD<Medicament>> {

    // private final SparkSession sparkSession;

    private final String outputPath;

    @Override
    public void call(JavaRDD<Medicament> medicamentJavaRDD) throws Exception {
        long ts = System.currentTimeMillis();
        log.info("micro-batch at stored in folder={}", ts);

        if(medicamentJavaRDD.isEmpty()){
            log.info("no data found!");
            return;
        }

        Dataset<Medicament> medicamentDataset = SparkSession.active().createDataset(
                medicamentJavaRDD.rdd(),
                Encoders.bean(Medicament.class)
        ).cache();


        medicamentDataset.printSchema();
        medicamentDataset.show(5, false);

        log.info("nb Medicament = {}", medicamentDataset.count());


        TextFileWriter writer = new TextFileWriter(outputPath + "/time=" + ts);
        writer.accept(medicamentDataset);

        medicamentDataset.unpersist();
        log.info("done");

    }
}

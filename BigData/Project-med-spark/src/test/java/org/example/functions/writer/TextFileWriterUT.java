package org.example.functions.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.writer.TextFileWriter;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class TextFileWriterUT {

    @Test
    public void testWriter() {
        String outputpath = "target/test-classes/data/output/testWriter";
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("testStatistic")
                .getOrCreate();
        Dataset<Medicament> stds = sparkSession.createDataset(
                List.of(
                        new Medicament("abc", "rue", "interdit"),
                        new Medicament("abc", "rue", "interdit"),
                        new Medicament("abc", "rue", "interdit")
                ),
                Encoders.bean(Medicament.class)
        );

        List<Medicament> medicaments = List.of(
                new Medicament("abc", "rue", "interdit"),
                new Medicament("abc", "rue", "interdit"),
                new Medicament("abc", "rue", "interdit")
        );

        TextFileWriter textFileWriter = new TextFileWriter(outputpath);
        textFileWriter.accept(stds);

        Dataset<Row> result = sparkSession.read().csv(outputpath);
        assertTrue(result.count() == medicaments.size());
    }
}

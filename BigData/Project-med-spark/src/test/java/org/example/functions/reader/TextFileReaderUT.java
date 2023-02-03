package org.example.functions.reader;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.reader.TextFileReader;
import org.example.medicaments.functions.writer.TextFileWriter;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TextFileReaderUT {
    @SneakyThrows
    @Test
    public void testReader() {
        String inputpath = "src/test/resources/Produits_dopants_20160317.csv";
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("testReader")
                .getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());

        TextFileReader textFileReader = new TextFileReader(inputpath, hdfs ,sparkSession);
        Dataset<Medicament> ds = textFileReader.get();
        assertTrue(ds.count() > 0);
        assertEquals(ds.columns().length, 3);

    }
}

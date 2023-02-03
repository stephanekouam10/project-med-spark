package org.example.medicaments.functions.reader;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.parser.MedicamentMapper;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Builder
public class TextFileReader implements Supplier<Dataset<Medicament>> {
    private final String inputPath;
    private final FileSystem hdfs;
    private final SparkSession sparkSession;

    private final MedicamentMapper mapper = new MedicamentMapper();

    @Override
    public Dataset<Medicament> get() {
        log.info("reading file at inputPath={}", inputPath);
        Path pathInput = new Path(inputPath);

        try {
            if (hdfs.exists(pathInput)) {

                FileStatus[] listFiles = hdfs.listStatus(pathInput);

                String[] inputPaths = Arrays.stream(listFiles)
                        .filter(t -> !t.isDirectory())
                        .map(f -> f.getPath().toString()).toArray(String[]::new);

                Dataset<Row> ds = sparkSession.read().option("delimiter", ";")
                        .option("header", "true").csv(inputPaths);
                return mapper.apply(ds);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sparkSession.emptyDataset(Encoders.bean(Medicament.class));
    }
}

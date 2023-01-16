package org.example.medicaments.functions.reader;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
@Builder
public class TextFileReader implements Supplier<Dataset<Row>> {
    private final String inputPath;
    private final SparkSession sparkSession;

    @Override
    public Dataset<Row> get() {
        log.info("reading file at inputPath={}", inputPath);

        Dataset<Row> ds = sparkSession.read().option("delimiter" , ";")
                .option("header" , "true").csv(inputPath) ;
        return ds;
    }
}

package org.example.medicaments.functions.parser;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.medicaments.beans.Medicament;

import java.util.function.Function;

import static org.apache.spark.sql.functions.count;

public class StatisticAggFunc implements Function<Dataset<Medicament>, Dataset<Row>> {


    @Override
    public Dataset<Row> apply(Dataset<Medicament> medicamentDataset) {
        return medicamentDataset.groupBy("Voie").agg(count("Substance").as("nb Substance/voie"));
    }
}

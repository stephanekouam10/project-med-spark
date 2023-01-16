package org.example.medicaments.functions.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.medicaments.beans.Medicament;

import java.util.function.Function;

public class MedicamentMapper implements Function<Dataset<Row>, Dataset<Medicament>> {
    private final RowToMedicamentFunc parser = new RowToMedicamentFunc();
    private final MapFunction<Row, Medicament> task = parser::apply;

    @Override
    public Dataset<Medicament> apply(Dataset<Row> inputDS) {
        return inputDS.map(task, Encoders.bean(Medicament.class));
    }
}

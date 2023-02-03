package org.example.medicaments.functions.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.example.medicaments.beans.Medicament;

import java.io.Serializable;
import java.util.function.Function;

public class RowToMedicamentSparkFunc implements MapFunction<Row, Medicament>, Serializable {

    @Override
    public Medicament call(Row row) throws Exception {
        String SUBSTANCE = row.getAs("Substance");
        String VOIE = row.getAs("Voie");
        String STATUT = row.getAs("Statut");

        return Medicament.builder()
                .substance(SUBSTANCE)
                .voie(VOIE)
                .statut(STATUT)
                .build();
    }
}

package org.example.medicaments.functions.parser;

import org.apache.spark.sql.Row;
import org.example.medicaments.beans.Medicament;

import java.io.Serializable;
import java.util.function.Function;

public class RowToMedicamentFunc implements Function<Row, Medicament>, Serializable {

    @Override
    public Medicament apply(Row row) {

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

package org.example.medicaments.functions.parser;

import org.apache.spark.sql.Row;
import org.example.medicaments.beans.Medicament;

import java.io.Serializable;
import java.util.function.Function;

public class TextToMedicamentFunc implements Function<String, Medicament>, Serializable {

    @Override
    public Medicament apply(String lines) {

        String[] fields = lines.split(";");

        String SUBSTANCE = fields[0];
        String VOIE = fields[1];
        String STATUT = fields[2];

        return Medicament.builder()
                .substance(SUBSTANCE)
                .voie(VOIE)
                .statut(STATUT)
                .build();
    }
}

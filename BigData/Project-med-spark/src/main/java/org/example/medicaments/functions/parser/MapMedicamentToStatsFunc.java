package org.example.medicaments.functions.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.beans.Stats;

import java.util.function.Function;

public class MapMedicamentToStatsFunc implements MapFunction<Medicament, Stats> {
    @Override
    public Stats call(Medicament medicament) throws Exception {
        return Stats.builder()
                .VoieKey(medicament.getVoie())
                .SubstanceCount(medicament.getSubstance())
                .build();
    }
}

package org.example.medicaments.functions.parser;

import org.apache.spark.api.java.function.MapFunction;
import org.example.medicaments.beans.Medicament;

public class MapFunctionKey implements MapFunction<Medicament, String> {
    @Override
    public String call(Medicament medicament) throws Exception {
        return medicament.getVoie();
    }
}

package org.example.medicaments.functions.funcs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.beans.Stats;
import org.example.medicaments.functions.parser.MapFunctionKey;
import org.example.medicaments.functions.parser.MapMedicamentToStatsFunc;

import static org.apache.spark.sql.functions.count;

import java.util.function.Function;

public class StatisticAggByFunc implements Function<Dataset<Medicament>, Dataset<Stats>> {

//
    @Override
    public Dataset<Stats> apply(Dataset<Medicament> medicamentDataset) {

        MapMedicamentToStatsFunc mapMedicamentToStatsFunc= new MapMedicamentToStatsFunc();
        Dataset<Stats> statsDataset = medicamentDataset.map(mapMedicamentToStatsFunc, Encoders.bean(Stats.class));

        MapFunctionKey mapFunctionKey = new MapFunctionKey();


        KeyValueGroupedDataset<String, Medicament> buckets = medicamentDataset.groupByKey(mapFunctionKey, Encoders.STRING());
//                .agg(count("Substance").as("nb Substance voie"));
        Encoder<Stats> encoder = Encoders.bean(Stats.class);

//        buckets.mapValues(encoder)
       // statsDataset = buckets.mapValues()  ;
         return statsDataset;
    }
}

package org.example.functions.statistic;

import org.apache.spark.sql.*;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.beans.Stats;
import org.example.medicaments.functions.parser.MapFunctionKey;
import org.example.medicaments.functions.parser.MapMedicamentToStatsFunc;
import org.example.medicaments.functions.funcs.StatisticAggByFunc;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StatisticAggByFuncUT {

    @Test
    public void testStatistic() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("testStatistic")
                .getOrCreate();
        Dataset<Medicament> stds = sparkSession.createDataset(
                List.of(
                        new Medicament("nicotine", "orale", "interdit"),
                        new Medicament("nicotine 200mg", "orale", "interdit en compe"),
                        new Medicament("aspirine", "anale", "interdit coours"),
                        new Medicament("paracetamol", "nasale", "interdit cpmm"),
                        new Medicament("paracetamol", "yeux", "interdit cpmm")
                ),
                Encoders.bean(Medicament.class)
        );

        StatisticAggByFunc f = new StatisticAggByFunc();
        MapFunctionKey mapFunctionKey = new MapFunctionKey();
        KeyValueGroupedDataset<String, Medicament> buckets = stds.groupByKey(mapFunctionKey, Encoders.STRING());

        MapMedicamentToStatsFunc mapMedicamentToStatsFunc = new MapMedicamentToStatsFunc();
        Encoder<Stats> encoder = Encoders.bean(Stats.class);

        // buckets.mapValues(mapFunctionKey, encoder).



    }
}

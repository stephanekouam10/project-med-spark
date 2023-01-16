package org.example.functions.statistic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.parser.StatisticAggFunc;
import org.example.medicaments.functions.parser.StatisticFunc;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class StastisticFuncAggUT {

    @Test
    public void testStatisticAgg() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("testStatisticAgg")
                .getOrCreate();
        StatisticAggFunc f = new StatisticAggFunc();
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

        Dataset<Row> result = f.apply(stds);

        assertEquals(4, result.count());
        assertEquals(2, result.filter("voie = 'orale'").first().getLong(1));
        assertEquals(1, result.filter("voie = 'nasale'").first().getLong(1));

    }
}

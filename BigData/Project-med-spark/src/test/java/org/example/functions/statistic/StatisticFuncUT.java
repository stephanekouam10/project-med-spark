package org.example.functions.statistic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.funcs.StatisticFunc;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class StatisticFuncUT {

    @Test
    public void testStatistic() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[2]")
                .appName("testStatistic")
                .getOrCreate();
        StatisticFunc f = new StatisticFunc();
        Dataset<Medicament> stds = sparkSession.createDataset(
                List.of(new Medicament("abc", "rue", "interdit")),
                Encoders.bean(Medicament.class)
        );

        Long expected = 1L;

        Long actual = f.apply(stds);

        assertThat(actual).isEqualTo(expected);

    }
}

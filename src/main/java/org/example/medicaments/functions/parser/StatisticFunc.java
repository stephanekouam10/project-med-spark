package org.example.medicaments.functions.parser;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.example.medicaments.beans.Medicament;
import static org.apache.spark.sql.functions.count;

import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
@Builder
public class StatisticFunc implements Function<Dataset<Medicament>, Long> {

    @Override
    public Long apply(Dataset<Medicament> medicamentDataset) {
        return medicamentDataset.count();
    }
}

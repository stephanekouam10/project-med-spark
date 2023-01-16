package org.example.medicaments.functions.writer;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.example.medicaments.beans.Medicament;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
@Builder
public class TextFileWriter implements Consumer<Dataset<Medicament>> {
    private final String outputPath;

    @Override
    public void accept(Dataset<Medicament> medicamentDataset) {
            medicamentDataset.write().mode(SaveMode.Overwrite).csv(outputPath);
    }
}

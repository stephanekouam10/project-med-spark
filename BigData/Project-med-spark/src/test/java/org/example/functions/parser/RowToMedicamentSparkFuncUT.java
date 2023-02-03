package org.example.functions.parser;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.example.medicaments.beans.Medicament;
import org.example.medicaments.functions.parser.RowToMedicamentFunc;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RowToMedicamentSparkFuncUT {

    @Test
    public void testCall() {
        RowToMedicamentFunc f = new RowToMedicamentFunc();
        StructType shema = new StructType(
                new StructField[]{
                        new StructField(
                                "Substance",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "Voie",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        ),

                        new StructField(
                                "Statut",
                                DataTypes.StringType,
                                true,
                                Metadata.empty()

                        )
                }
        );

        String values[] = {"fentanyl citrate", "sublinguale", "Interdit en comp�tition"} ;
        Row row = new GenericRowWithSchema(values , shema);
        Medicament expected = Medicament.builder()
                .substance("fentanyl citrate")
                .voie("sublinguale")
                .statut("Interdit en comp�tition")
                .build();

        Medicament actual = f.apply(row);

        assertThat(actual).isEqualTo(expected);
    }
}

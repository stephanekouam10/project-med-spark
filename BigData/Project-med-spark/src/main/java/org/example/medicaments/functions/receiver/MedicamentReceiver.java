package org.example.medicaments.functions.receiver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.medicaments.beans.Medicament;

import org.apache.spark.api.java.function.Function;
import org.example.medicaments.functions.parser.TextToMedicamentFunc;
import org.example.medicaments.types.MedicamentFileInputFormat;
import org.example.medicaments.types.MedicamentLongWritable;
import org.example.medicaments.types.MedicamentText;

import java.util.function.Supplier;

@AllArgsConstructor
@Builder
@Slf4j
@RequiredArgsConstructor
public class MedicamentReceiver implements Supplier<JavaDStream<Medicament>> {

    private final String hdfsInputPathStr;
    private final JavaStreamingContext jsc;

    Function<Path, Boolean> filter = hdfsPath -> {
        return !hdfsPath.getName().startsWith(".") &&
                !hdfsPath.getName().startsWith("_") &&
                !hdfsPath.getName().startsWith(".tmp");
    };

    private final TextToMedicamentFunc textToMedicamentFunc = new TextToMedicamentFunc();
    private final Function<String, Medicament> mapper = textToMedicamentFunc::apply;

    @Override
    public JavaDStream<Medicament> get() {
        JavaPairInputDStream<MedicamentLongWritable, MedicamentText> inputDStream = jsc
                .fileStream(
                        hdfsInputPathStr,
                        MedicamentLongWritable.class,
                        MedicamentText.class,
                        MedicamentFileInputFormat.class,
                        filter,
                        true
                );
        inputDStream.print();
//        JavaDStream<String> javaDStream = inputDStream.map(tuple -> tuple._2().toString());
//        JavaDStream<Medicament> medicaments = mapToMedicament(javaDStream);

        return inputDStream.map(t -> t._2().toString()).map(mapper);
    }

    public static JavaDStream<Medicament> mapToMedicament(JavaDStream<String> javaDStream) {
        return javaDStream.map(line -> {
            String[] fields = line.split(";");
            return new Medicament(fields[0], fields[1], fields[2]);
        });
    }
}

package org.example.functions;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.StreamingMain;
import org.example.StreamingV2Main;
import org.example.medicaments.functions.reader.HdfsTextFileReader;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class StreamingMainTest {

    String outputPathStr = ConfigFactory.load().getString("app.path.output");

    @Test
    public void test() throws IOException, InterruptedException {

        FileSystem localFs = FileSystem.getLocal(new Configuration());
        log.info("fileSystem used in the test : localFs.getScheme = {}", localFs.getScheme());

        StreamingMain.main(new String[0]);

        Path outputPath = new Path(outputPathStr);
        Stream<Path> jsonFilePaths = Arrays.stream(localFs.listStatus(outputPath))
                .map(FileStatus::getPath)
                .filter(p -> p.getName().startsWith("part-") && p.toString().endsWith(".csv"));

        List<String> lines = jsonFilePaths
                .flatMap(outputJsonFilePath -> new HdfsTextFileReader(localFs, outputJsonFilePath).get())
                .collect(Collectors.toList());

        assertThat(lines)
                .isNotEmpty()
                .hasSize(1)
                .contains("Interdit en comp�tition,fentanyl citrate,sublinguale");
    }
}

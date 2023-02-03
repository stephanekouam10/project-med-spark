package org.example.functions;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.Main;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class MainTest {
    String OutputPathStr = ConfigFactory.load().getString("app.path.output");

    @Test
    public void test() throws IOException, InterruptedException {

        Main.main(new String[0]);
        Path output = Paths.get(OutputPathStr);
        Stream<Path> jsonFilePaths = Files.list(output).filter(p -> p.getFileName().toString().startsWith("part-") && p.toString().endsWith(".csv"));
        List<String> lines = jsonFilePaths.flatMap(outputJsonfilepath -> {
            Stream<String> jsonFileContent = Stream.empty();
            try {
                jsonFileContent = Files.lines(outputJsonfilepath);
            } catch (IOException e) {
                log.info("ccc");
            }
            return jsonFileContent;
        }).collect(Collectors.toList());
        assertThat(lines).isNotEmpty().contains("Interdit en comp�tition,fentanyl citrate,sublinguale");
        ;
    }
}
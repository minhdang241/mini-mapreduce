package mr;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MrSequentialTest {
    @TempDir
    Path tempDir;

    @Test
    void testWordCountSingleFile() throws Exception {
        Path input = tempDir.resolve("input.txt");
        Files.writeString(input, "hello world hello");

        String output = tempDir.resolve("mr-out-0").toString();
        WordCount wc = new WordCount();
        MrSequential.run(wc, wc, output, input.toString());
        List<String> lines = Files.readAllLines(Path.of(output));
        assertTrue(lines.contains("hello 2"));
        assertTrue(lines.contains("world 1"));
        assertEquals(2, lines.size());
    }
}

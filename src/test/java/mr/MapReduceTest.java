package mr;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MapReduceTest {

  private final List<String> inputFiles = List.of("data/pg-1.txt", "data/pg-2.txt");
  private final int nReduce = 2;
  private Coordinator coordinator;
  private Thread worker1;
  private Thread worker2;

  @BeforeEach
  void setup() {
    // Clean up any old output files before each test
    cleanupFiles();
  }

  @AfterEach
  void teardown() {
    joinQuietly(worker1);
    joinQuietly(worker2);
    if (coordinator != null) {
      coordinator.stopServer();
    }
    cleanupFiles();
  }

  private void joinQuietly(Thread thread) {
    if (thread != null) {
      try {
        thread.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void cleanupFiles() {
    File dir = new File(".");
    File[] files = dir.listFiles();
    if (files == null) {
      return;
    }
    for (File f : files) {
      if (f.getName().startsWith("mr-") || f.getName().startsWith("map-") || f.getName()
          .startsWith("reduce-")) {
        try {
          Files.deleteIfExists(f.toPath());
        } catch (IOException e) {
          throw new RuntimeException("Failed to delete temp file: " + f.getName(), e);
        }
      }
    }
  }

  @Test
  void testBasicWordCount() throws Exception {
    // 1. Calculate the expected "Golden Master" result sequentially
    Map<String, Integer> expectedCounts = runSequentialWordCount(inputFiles);

    // 2. Start the gRPC Coordinator
    coordinator = new Coordinator(inputFiles, nReduce, 0);
    int coordinatorPort = coordinator.startServer();

    // 3. Start 2 Workers in background threads
    worker1 = new Thread(() -> new Worker(new WordCount(), new WordCount(), coordinatorPort).run());
    worker2 = new Thread(() -> new Worker(new WordCount(), new WordCount(), coordinatorPort).run());
    worker1.start();
    worker2.start();

    // 4. Wait for the distributed job to complete
    coordinator.awaitDone();
    joinQuietly(worker1);
    joinQuietly(worker2);
    coordinator.stopServer();

    // 5. Read the distributed output files
    Map<String, Integer> actualCounts = readDistributedOutput();

    // 6. Assert that our distributed system got the exact same result!
    assertEquals(expectedCounts, actualCounts,
        "Distributed output does not match sequential output!");
    System.out.println("✅ Basic WordCount Test Passed!");
  }

  // --- Helper Methods ---

  private Map<String, Integer> runSequentialWordCount(List<String> files) throws Exception {
    Map<String, Integer> counts = new HashMap<>();
    WordCount wc = new WordCount();

    // Map
    List<KeyValue> allKvs = new ArrayList<>();
    for (String file : files) {
      String content = Files.readString(Paths.get(file));
      allKvs.addAll(wc.map(file, content));
    }

    // Reduce
    Map<String, List<String>> grouped = new HashMap<>();
    for (KeyValue kv : allKvs) {
      grouped.computeIfAbsent(kv.key, k -> new ArrayList<>()).add(kv.value);
    }
    for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
      counts.put(entry.getKey(), Integer.parseInt(wc.reduce(entry.getKey(), entry.getValue())));
    }
    return counts;
  }

  private Map<String, Integer> readDistributedOutput() throws Exception {
    Map<String, Integer> counts = new HashMap<>();
    for (int i = 0; i < nReduce; i++) {
      File outFile = new File("mr-out-" + i);
      if (outFile.exists()) {
        List<String> lines = Files.readAllLines(outFile.toPath());
        for (String line : lines) {
          String[] parts = line.split(" ");
          counts.put(parts[0], Integer.parseInt(parts[1]));
        }
      }
    }
    return counts;
  }
}

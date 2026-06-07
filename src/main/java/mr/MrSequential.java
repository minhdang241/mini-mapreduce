package mr;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MrSequential {

  public static void run(MapFunc mapf, ReduceFunc reducef, String outputFile, String... inputFile)
      throws Exception {
    List<KeyValue> intermediate = new ArrayList<>();
    for (String file : inputFile) {
      String contents = Files.readString(Paths.get(file));
      intermediate.addAll(mapf.map(file, contents));
    }
    intermediate.sort(Comparator.comparing(kv -> kv.key));
    try (PrintWriter writer = new PrintWriter(outputFile)) {
      int i = 0;
      while (i < intermediate.size()) {
        String key = intermediate.get(i).key;
        List<String> values = new ArrayList<>();
        while (i < intermediate.size() && intermediate.get(i).key.equals(key)) {
          values.add(intermediate.get(i).value);
          i++;
        }
        String result = reducef.reduce(key, values);
        writer.printf("%s %s\n", key, result);
      }
    }
  }
}

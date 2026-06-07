package mr;


import java.util.ArrayList;
import java.util.List;

public class WordCount implements MapFunc, ReduceFunc {

  @Override
  public List<KeyValue> map(String filename, String contents) {
    String[] words = contents.split("[^a-zA-Z]+");
    List<KeyValue> result = new ArrayList<>();
    for (String word : words) {
      if (!word.isEmpty()) {
        KeyValue kv = new KeyValue();
        kv.key = word;
        kv.value = "1";
        result.add(kv);
      }
    }
    return result;
  }

  @Override
  public String reduce(String key, List<String> values) {
    return String.valueOf(values.size());
  }
}

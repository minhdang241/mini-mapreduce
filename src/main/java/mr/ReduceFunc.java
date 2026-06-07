package mr;

import java.util.List;

public interface ReduceFunc {

  String reduce(String key, List<String> values);
}

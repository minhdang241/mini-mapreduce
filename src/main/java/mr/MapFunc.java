package mr;

import java.util.List;

public interface MapFunc {
    List<KeyValue> map(String filename, String contents);
}

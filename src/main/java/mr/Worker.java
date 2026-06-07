package mr;

import com.google.gson.Gson;

import java.util.UUID;

public class Worker {

  private final MapFunc mapFunc;
  private final ReduceFunc reduceFunc;
  private final String workerId;
  private final Gson gson;
  private Coordinator coordinator;

  public Worker(MapFunc mapFunc, ReduceFunc reduceFunc) {
    this.mapFunc = mapFunc;
    this.reduceFunc = reduceFunc;
    this.workerId = UUID.randomUUID().toString();
    this.gson = new Gson();
  }
}

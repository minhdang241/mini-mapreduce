package mr;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import com.mr.proto.CoordinatorGrpc;
import com.mr.proto.DoneRequest;
import com.mr.proto.TaskReply;
import com.mr.proto.TaskRequest;
import com.mr.proto.TaskType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class Worker {

  private final MapFunc mapFunc;
  private final ReduceFunc reduceFunc;
  private final String workerId;
  private final Gson gson;
  private final CoordinatorGrpc.CoordinatorBlockingStub stub;

  public Worker(MapFunc mapFunc, ReduceFunc reduceFunc) {
    this(mapFunc, reduceFunc, 50051);
  }

  public Worker(MapFunc mapFunc, ReduceFunc reduceFunc, int coordinatorPort) {
    this.mapFunc = mapFunc;
    this.reduceFunc = reduceFunc;
    this.workerId = UUID.randomUUID().toString();
    this.gson = new Gson();
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", coordinatorPort)
        .usePlaintext()
        .build();
    this.stub = CoordinatorGrpc.newBlockingStub(channel);
  }

  private static int ihash(String key) {
    return (key.hashCode() & 0x7fffffff);
  }

  public void run() {
    System.out.println("Worker " + workerId + " started.");
    while (true) {
      try {
        TaskRequest req = TaskRequest.newBuilder().setWorkerId(workerId).build();
        TaskReply reply = stub.getTask(req);
        switch (reply.getTaskType()) {
          case MAP:
            doMap(reply);
            stub.reportDone(DoneRequest.newBuilder()
                .setTaskType(TaskType.MAP)
                .setTaskId(reply.getTaskId())
                .build());
            break;
          case REDUCE:
            doReduce(reply);
            stub.reportDone(DoneRequest.newBuilder()
                .setTaskType(TaskType.REDUCE)
                .setTaskId(reply.getTaskId())
                .build());
            break;
          case WAIT:
            Thread.sleep(1000);
            break;
          case SHUTDOWN:
            System.out.println("Worker " + workerId + " shutting down.");
            return;
        }
      } catch (Exception e) {
        System.err.println("Worker " + workerId + " encountered an error: " + e.getMessage());
      }
    }
  }

  private void doMap(TaskReply reply) throws IOException {
    System.out.println("Worker " + workerId + " processing map task " + reply.getTaskId());
    String contents = Files.readString(Paths.get(reply.getFilename()));
    List<KeyValue> kvs = mapFunc.map(reply.getFilename(), contents);
    List<List<KeyValue>> buckets = new ArrayList<>(reply.getNReduce());
    for (int i = 0; i < reply.getNReduce(); i++) {
      buckets.add(new ArrayList<>());
    }
    for (KeyValue kv : kvs) {
      int reduceId = ihash(kv.key) % reply.getNReduce();
      buckets.get(reduceId).add(kv);
    }
    for (int i = 0; i < reply.getNReduce(); i++) {
      String finalFileName = "mr-" + reply.getTaskId() + "-" + i;
      File tempFile = File.createTempFile("map-", "-tmp", new File("."));
      String json = gson.toJson(buckets.get(i));
      Files.writeString(tempFile.toPath(), json);
      Files.move(tempFile.toPath(), Paths.get(finalFileName), StandardCopyOption.ATOMIC_MOVE);
    }
  }

  private void doReduce(TaskReply reply) throws IOException {
    System.out.println("Worker " + workerId + " processing reduce task " + reply.getTaskId());
    TreeMap<String, List<String>> groupedData = new TreeMap<>();
    Type listType = new TypeToken<ArrayList<KeyValue>>() {
    }.getType();
    for (int m = 0; m < reply.getNMap(); m++) {
      Path filePath = Paths.get("mr-" + m + "-" + reply.getTaskId());
      if (Files.exists(filePath)) {
        String json = Files.readString(filePath);
        List<KeyValue> kvs = gson.fromJson(json, listType);
        for (KeyValue kv : kvs) {
          groupedData.computeIfAbsent(kv.key, k -> new ArrayList<>()).add(kv.value);
        }
      }
    }
    File tempFile = File.createTempFile("reduce-", "-tmp", new File("."));
    StringBuilder outputContent = new StringBuilder();
    for (Map.Entry<String, List<String>> entry : groupedData.entrySet()) {
      String reducedValue = reduceFunc.reduce(entry.getKey(), entry.getValue());
      outputContent.append(entry.getKey()).append(" ").append(reducedValue).append("\n");
    }
    Files.writeString(tempFile.toPath(), outputContent.toString());
    String finalFileName = "mr-out-" + reply.getTaskId();
    Files.move(tempFile.toPath(), Paths.get(finalFileName), StandardCopyOption.ATOMIC_MOVE);
  }

}

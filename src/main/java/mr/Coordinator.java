package mr;

import com.mr.proto.CoordinatorGrpc;
import com.mr.proto.DoneReply;
import com.mr.proto.DoneRequest;
import com.mr.proto.TaskReply;
import com.mr.proto.TaskRequest;
import com.mr.proto.TaskType;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Coordinator extends CoordinatorGrpc.CoordinatorImplBase {

  private final Task[] mapTasks;
  private final Task[] reduceTasks;
  private final int nMap;
  private final int nReduce;
  private final int requestedPort;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final CountDownLatch doneLatch = new CountDownLatch(1);
  private Server server;
  private Phase currentPhase = Phase.MAP;

  public Coordinator(List<String> files, int nReduce) {
    this(files, nReduce, 50051);
  }

  public Coordinator(List<String> files, int nReduce, int port) {
    this.nMap = files.size();
    this.nReduce = nReduce;
    this.requestedPort = port;
    this.mapTasks = new Task[nMap];
    for (int i = 0; i < nMap; i++) {
      mapTasks[i] = new Task(i, files.get(i));
    }
    this.reduceTasks = new Task[this.nReduce];
    for (int i = 0; i < nReduce; i++) {
      reduceTasks[i] = new Task(i, null);
    }
    startTimeoutThread();
  }

  @Override
  public synchronized void getTask(TaskRequest request,
      StreamObserver<TaskReply> responseObserver) {
    TaskReply.Builder replyBuilder = TaskReply.newBuilder();
    if (currentPhase == Phase.MAP) {
      Task t = assignTask(mapTasks);
      if (t != null) {
        replyBuilder.setTaskType(TaskType.MAP)
            .setTaskId(t.id)
            .setFilename(t.filename)
            .setNMap(nMap)
            .setNReduce(nReduce);

      } else if (allTasksCompleted(mapTasks)) {
        currentPhase = Phase.REDUCE;
        Task r = assignTask(reduceTasks);
        if (r != null) {
          replyBuilder.setTaskType(TaskType.REDUCE)
              .setTaskId(r.id)
              .setNMap(nMap)
              .setNReduce(nReduce);
        } else {
          replyBuilder.setTaskType(TaskType.WAIT);
        }
      }
    } else if (currentPhase == Phase.REDUCE) {
      Task t = assignTask(reduceTasks);
      if (t != null) {
        replyBuilder.setTaskType(TaskType.REDUCE)
            .setTaskId(t.id)
            .setNMap(nMap)
            .setNReduce(nReduce);
      } else if (allTasksCompleted(reduceTasks)) {
        currentPhase = Phase.DONE;
        doneLatch.countDown();
        replyBuilder.setTaskType(TaskType.SHUTDOWN);
      } else {
        replyBuilder.setTaskType(TaskType.WAIT);
      }
    } else {
      replyBuilder.setTaskType(TaskType.SHUTDOWN);
    }
    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }

  @Override
  public synchronized void reportDone(DoneRequest request,
      StreamObserver<DoneReply> responseObserver) {
    int taskId = request.getTaskId();
    Task[] tasks = (request.getTaskType() == TaskType.MAP) ? mapTasks : reduceTasks;
    if (taskId >= 0 && taskId < tasks.length) {
      tasks[taskId].status = TaskStatus.COMPLETED;
      System.out.println(request.getTaskType() + " task " + taskId + " completed.");
    }
    responseObserver.onNext(DoneReply.newBuilder().build());
    responseObserver.onCompleted();
  }

  public int startServer() throws Exception {
    this.server = ServerBuilder.forPort(requestedPort)
        .addService(this)
        .build()
        .start();
    System.out.println("Coordinator started on port " + server.getPort());
    return server.getPort();
  }

  public void stopServer() {
    if (server != null) {
      server.shutdown();
    }
    scheduler.shutdownNow();
  }

  private boolean allTasksCompleted(Task[] tasks) {
    for (Task t : tasks) {
      if (t.status != TaskStatus.COMPLETED) {
        return false;
      }
    }
    return true;
  }


  private Task assignTask(Task[] tasks) {
    for (Task t : tasks) {
      if (t.status == TaskStatus.IDLE) {
        t.status = TaskStatus.IN_PROGRESS;
        t.startTime = System.currentTimeMillis();
        return t;
      }
    }
    return null;
  }

  public synchronized boolean done() {
    return currentPhase == Phase.DONE;
  }

  public void awaitDone() throws InterruptedException {
    doneLatch.await();
  }

  private void startTimeoutThread() {
    scheduler.scheduleAtFixedRate(() -> {
      if (done()) {
        scheduler.shutdown();
      } else {
        checkTimeouts();
      }
    }, 1, 1, TimeUnit.SECONDS);
  }

  private void checkTimeouts() {
    Task[] tasks = (currentPhase == Phase.MAP) ? mapTasks : reduceTasks;
    long now = System.currentTimeMillis();
    long threshold = 10000; // 10 seconds
    for (Task t : tasks) {
      if (t.status == TaskStatus.IN_PROGRESS && (now - t.startTime) > threshold) {
        t.status = TaskStatus.IDLE;
        System.out.println("Task " + t.id + " timed out.");
      }
    }
  }


  private enum Phase {MAP, REDUCE, DONE}

  private enum TaskStatus {IDLE, IN_PROGRESS, COMPLETED}

  private static class Task {

    int id;
    TaskStatus status;
    long startTime;
    String filename;

    Task(int id, String filename) {
      this.id = id;
      this.status = TaskStatus.IDLE;
      this.filename = filename;
    }
  }
}

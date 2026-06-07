package mr;

import java.util.Arrays;
import java.util.List;

public class CoordinatorMain {

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Usage: java mr.CoordinatorMain <input-file1> <input-file2> ...");
      System.exit(1);
    }
    List<String> files = Arrays.asList(args);
    int nReduce = 3; // You can adjust this as needed
    Coordinator coordinator = new Coordinator(files, nReduce);
    try {
      coordinator.startServer();
    } catch (Exception e) {
      System.err.println("Error starting coordinator: " + e.getMessage());
      System.exit(1);
    }
    try {
      coordinator.awaitDone();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    coordinator.stopServer();
    System.out.println("All tasks completed. Coordinator exiting.");
  }

}

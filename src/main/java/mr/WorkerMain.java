package mr;

public class WorkerMain {

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: WorkerMain <config_file>");
      System.exit(1);
    }
    // Initialize and start the worker
    String pluginClassName = args[0];
    try {
      Class<?> pluginClass = Class.forName(pluginClassName);
      Object pluginInstance = pluginClass.getDeclaredConstructor().newInstance();
      MapFunc mapFunc = (MapFunc) pluginInstance;
      ReduceFunc reduceFunc = (ReduceFunc) pluginInstance;
      Worker worker = new Worker(mapFunc, reduceFunc);
      worker.run();
    } catch (Exception e) {
      System.err.println("Failed to load plugin or start worker: " + e.getMessage());
      System.exit(1);
    }
  }

}



import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.google.gson.JsonObject;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class Consumer {

  public static final Map<Integer, List<JsonObject>> liftRecordsMap = new ConcurrentHashMap<>();
  private static Properties properties = new Properties();
  private static ConnectionFactory connectionFactory;
  private static String rabbitMQName;
  private static int numberOfThread;
  private static int basicqos;
  public static void main(String[] args) {

    try (InputStream inputStream = Consumer.class.getClassLoader().getResourceAsStream("rabbitmq.conf")) {
      if ( inputStream == null) {
        throw new RuntimeException("Fail to config rabbitMq because unable to read config file");
      }

      // init connection
      properties.load(inputStream);
      connectionFactory = new ConnectionFactory();
      connectionFactory.setHost(properties.getProperty("host"));
      connectionFactory.setPort(Integer.parseInt(properties.getProperty("port")));
      connectionFactory.setUsername(properties.getProperty("username"));
      connectionFactory.setPassword(properties.getProperty("password"));
      rabbitMQName = properties.getProperty("queueName");
      basicqos = Integer.parseInt(properties.getProperty("basicQos"));
      numberOfThread = Integer.parseInt(properties.getProperty("numberOfThread"));


      if (args.length > 0) {
        try {
          numberOfThread = Integer.parseInt(args[0]);
          basicqos = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
          System.out.println("Parameter error");
          System.exit(1);
        }
      }

      System.out.println("Init Servlet  with numberOfThread :" + numberOfThread + ", and basicqos : " + basicqos);

      Connection connection = connectionFactory.newConnection();
      ExecutorService multiThreadPool = Executors.newFixedThreadPool(numberOfThread);


      // Init Dynamo DB
      DynamoDBHelper.init();
      DynamoDBHelper.createLiftRecordTable();

      // start channels
      for (int num = 0; num < numberOfThread; num ++) {
        multiThreadPool.execute(new ConsumerThread(connection, rabbitMQName, basicqos));
      }

      System.out.println("Successfully Init Servlet");


      // last steps
      ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
      checkNumberOfRecordInDDB(scheduledExecutorService);
      shutDown(scheduledExecutorService);


    } catch (Exception e) {
      // if init fail, we will throw new exception
      System.out.println(" Fail to Init Servlet");
      throw new RuntimeException(e);
    }
  }



  private static void checkNumberOfRecordInDDB(ScheduledExecutorService scheduledExecutorService){
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      long successfulWrites = DynamoDBHelper.getSuccessCount();
      System.out.println("Total Number of Record Saved To DDB " + successfulWrites);
    }, 0, 1, TimeUnit.SECONDS);
  }

  private static void shutDown(ScheduledExecutorService scheduledExecutorService){
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      DynamoDBHelper.flush();
      scheduledExecutorService.shutdown();
      try {
        if (!scheduledExecutorService.awaitTermination(60, TimeUnit.SECONDS)) {
          scheduledExecutorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        scheduledExecutorService.shutdownNow();
      }
    }));
  }



}

import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.awssdk.regions.Region;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.annotation.WebServlet;
import java.io.IOException;
import model.LiftRideRecord;
import model.Message;
import com.google.gson.Gson;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {

  private Gson gson = new Gson();
  private Properties properties = new Properties();
  private String rabbitMQName;
  private ConnectionFactory connectionFactory;
  private BlockingQueue<Channel> channelPool;
  private Connection connection;
  private int numberOfChannel;

  private final int validUrlPathLength = 8;
  private final int validUrlPathResortNumberPosition = 1;
  private final int validUrlPathSeasonPosition = 2;
  private final int validUrlPathSeasonIDPosition = 3;
  private final int validUrlPathDaysPosition = 4;
  private final int validUrlPathDaysIDPosition = 5;
  private final int validUrlPathSkiersPosition = 6;
  private final int validUrlPathSkiersIDPosition = 7;

  private static int SEASON_ID_INDEX = 0;
  private static int LIFT_ID_INDEX = 1;
  private static int DAY_ID_INDEX = 2;
  private static int TIME_ID_INDEX = 3;


  public static String tableName = "LiftRideRecordTable";

  public static Region region = Region.US_WEST_2;

  public static DynamoDbClient dynamoDbClient;

  public static String AWS_ACCESS_KEY_ID;

  public static String AWS_SECRET_ACCESS_KEY;


  @Override
  public void init() throws ServletException {
    // init rabbitmq connection and thread pool
    super.init();

    if (dynamoDbClient == null) {
      try {
        // local test
//        dynamoDbClient = DynamoDbClient.builder()
//                .endpointOverride(URI.create("http://localhost:8000"))
//                .region(Region.US_WEST_1)
//                .credentialsProvider(StaticCredentialsProvider.create(
//                        AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)))
//                .build();

        try{
          dynamoDbClient = DynamoDbClient.builder().region(region).build();
        } catch ( Exception e) {
          System.err.println("Fail to init Dynamo DB client" + e.getClass().getName() + ": " + e.getMessage());
        }

        System.out.println("Successfully initialized DynamoDB client to connect to local instance.");
      } catch (Exception e) {
        System.err.println("Failed to initialize DynamoDB client: " + e.getClass().getName() + ": " + e.getMessage());
      }
    }


    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("rabbitmq.conf")) {
      if ( inputStream == null) {
        throw new ServletException("Fail to config rabbitMq because unable to read config file");
      }

      properties.load(inputStream);
      connectionFactory = new ConnectionFactory();
      connectionFactory.setHost(properties.getProperty("host"));
      connectionFactory.setPort(Integer.parseInt(properties.getProperty("port")));
      connectionFactory.setUsername(properties.getProperty("username"));
      connectionFactory.setPassword(properties.getProperty("password"));
      rabbitMQName = properties.getProperty("queueName");
      numberOfChannel = Integer.parseInt(properties.getProperty("numberOfChannel"));

      connection = connectionFactory.newConnection();
      channelPool = new LinkedBlockingQueue<>();

      for (int i=0; i < numberOfChannel; i++ ) {
        Channel channel = connection.createChannel();
        channel.queueDeclare(rabbitMQName, false, false, false, null);
        channelPool.add(channel);
      }

      System.out.println("Successful to Init Servlet ");

    } catch (Exception e) {
      // if init fail, we will throw new exception
      System.out.println(" Fail to Init Servlet");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    try {
      // Close all channel
      for (Channel channel : channelPool) {
        if (channel.isOpen()) {
          channel.close();
        }
      }
      // close connection
      if (connection != null && connection.isOpen()) {
        connection.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String path = request.getPathInfo();
    if (path.matches("/\\d+$")) {
      String[] segments = path.split("/");
      String skierId = segments[1];

      String jsonResponse = getSkierVerticalTotals(skierId);

      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      response.getWriter().write(jsonResponse);
    } else if (path.matches("/\\d+/seasons/\\d+/days/\\d+/skiers/\\d+$")) {
      String[] segments = path.split("/");

      String resortId = segments[1];
      String seasonId = segments[3];
      String dayId = segments[5];
      String skierId = segments[7];

      String jsonResponse = getSkierDayVertical(resortId, seasonId, dayId, skierId);

      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      response.getWriter().write(jsonResponse);
    }
    else {
      ObjectMapper mapper = new ObjectMapper();
      ObjectNode jsonResponse = mapper.createObjectNode();

      jsonResponse.put("error", "No matching route found");
      jsonResponse.put("path", path);

      ObjectNode queryParams = mapper.createObjectNode();
      request.getParameterMap().forEach((key, value) -> {
        queryParams.put(key, value[0]);
      });
      jsonResponse.set("queryParams", queryParams);

      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      response.getWriter().write(jsonResponse.toString());
    }
  }


  private static String getSkierDayVertical(String resortId, String seasonId, String dayId, String skierId) {
    System.out.println("Querying for total ski day vertical for a skier");
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode jsonResponse = mapper.createObjectNode();
    try {
      ScanRequest scanRequest = ScanRequest.builder()
              .tableName(tableName)
              .filterExpression("resortID = :v_resort AND skierID = :v_skier")
              .expressionAttributeValues(Map.of(
                      ":v_resort", AttributeValue.builder().s(resortId).build(),
                      ":v_skier", AttributeValue.builder().s(skierId).build()))
              .build();


      List<Map<String, AttributeValue>> items = dynamoDbClient.scan(scanRequest).items();
      System.out.println(items.size());

      int totalVertical = 0;

      for (Map<String, AttributeValue> item : items) {
        String[] parts = item.get("liftInfo").s().split(":");
        String seasonIdInRecord = parts[SEASON_ID_INDEX];
        String dayIdInRecord = parts[DAY_ID_INDEX];

        if (seasonIdInRecord.equals(seasonId) && dayIdInRecord.equals(dayId)) {
          int liftID = Integer.parseInt(parts[LIFT_ID_INDEX]);
          totalVertical += liftID * 10;
        }
      }
      jsonResponse.put("The total ski day vertical for a skier: ", totalVertical);
      return mapper.writeValueAsString(jsonResponse);
    } catch (Exception e) {
      System.err.println("An error occurred: " + e);
      jsonResponse.removeAll();
      jsonResponse.put("error", "An error occurred while processing your request");
      try {
        return mapper.writeValueAsString(jsonResponse);
      } catch (JsonProcessingException jsonProcessingException) {
        return "{\"error\":\"Failed to process error response\"}";
      }
    }
  }

  private static String getSkierVerticalTotals(String skierId) {
    // get the total vertical for the skier for specified seasons at the specified resort
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode jsonResponse = mapper.createObjectNode();
    int totalVertical = 0;

    System.out.println("Querying for total vertical for skier " + skierId);
    try {
      QueryRequest queryRequest = QueryRequest.builder()
              .tableName(tableName)
              .keyConditionExpression("skierID = :v_id")
              .expressionAttributeValues(Map.of(":v_id", AttributeValue.builder().s(skierId).build()))
              .build();

      List<Map<String, AttributeValue>> items = dynamoDbClient.query(queryRequest).items();

      for (Map<String, AttributeValue> item : items) {
        String info = item.get("liftInfo").s();
        String[] parts = info.split(":");
        int liftID = Integer.parseInt(parts[LIFT_ID_INDEX]);
        totalVertical += liftID * 10;
      }

      String key = "The total vertical for the skier for specified seasons at the specified resort: ";
      jsonResponse.put(key, totalVertical);

      return mapper.writeValueAsString(jsonResponse);

    } catch (Exception e) {
      jsonResponse.put("error", "An error occurred querying DynamoDB: " + e.getMessage());
      try {
        return mapper.writeValueAsString(jsonResponse);
      } catch (Exception jsonProcessingException) {
        System.err.println("An error occurred while generating error response: " + jsonProcessingException.getMessage());
        jsonProcessingException.printStackTrace();
        return "{\"error\":\"Failed to process error response\"}";
      }
    }
  }





  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {

    PrintWriter printWriter = response.getWriter();
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    String urlPath = request.getPathInfo();

    // check we have a URL!
    if(urlPath == null || urlPath.isEmpty()){
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    // check url is valid
    if (!isUrlValid(urlParts)) {
      Message message = new Message("The request url is invalid");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      printWriter.write(response.getStatus() + gson.toJson(message));
      return;
    }

    try {
      StringBuilder stringBuilder = new StringBuilder();
      String str;
      while ((str = request.getReader().readLine()) != null) {
        stringBuilder.append(str);
      }
      LiftRideRecord liftRideRecord = gson.fromJson(stringBuilder.toString(), LiftRideRecord.class);
      liftRideRecord.setResortID( Integer.parseInt(urlParts[validUrlPathResortNumberPosition]));
      liftRideRecord.setSeasonID(urlParts[validUrlPathSeasonIDPosition]);
      liftRideRecord.setDayID(urlParts[validUrlPathDaysIDPosition]);
      liftRideRecord.setSkierID(Integer.parseInt(urlParts[validUrlPathSkiersIDPosition]));


      if (!isPostValid(liftRideRecord)) {
        Message message = new Message("The request body is invalid");
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        printWriter.write(response.getStatus() + gson.toJson(message));
        return;
      }

      // send to rabbitmq
      try {
        Channel channel = channelPool.take();

        // convert to JsonPrimitive
        JsonObject messageObject = new JsonObject();
        messageObject.add("skierID", new JsonPrimitive(String.valueOf(Integer.parseInt(urlParts[validUrlPathSkiersIDPosition]))));
        messageObject.add("resortID", new JsonPrimitive(String.valueOf(Integer.parseInt(urlParts[validUrlPathResortNumberPosition]))));
        messageObject.add("liftID", new JsonPrimitive(String.valueOf(liftRideRecord.getLiftID())));
        messageObject.add("seasonID", new JsonPrimitive(urlParts[validUrlPathSeasonIDPosition]));
        messageObject.add("dayID", new JsonPrimitive(urlParts[validUrlPathDaysIDPosition]));
        messageObject.add("time", new JsonPrimitive(String.valueOf(liftRideRecord.getTime())));

        channel.basicPublish("", rabbitMQName, null, messageObject.toString().getBytes());
//        System.out.println(" Successful Sent message with record: " + liftRideRecord);
        channelPool.add(channel);
      } catch (Exception e) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        printWriter.write(response.getStatus()  + " Fail to send message to RabbitMq");
        return;
      }

      response.setStatus(HttpServletResponse.SC_OK);
      printWriter.write(response.getStatus() + gson.toJson(stringBuilder));
    } catch ( Exception e) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      printWriter.write(response.getStatus()  + "The request body is invalid with exception");
    }

  }

  private boolean isUrlValid(String[] urlPath) {
    // validate the request url path according to the API spec
    // example valid path
    // "/skier/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}"

    List<String> validDays = Arrays.asList("1", "2", "3");
    String seasonID = "2024";
    int resortID_Max = 10;
    int resortID_Min = 1;
    int skierID_Max = 100000;
    int skierID_Min = 1;

    if (urlPath.length == validUrlPathLength) {
      return urlPath[validUrlPathSeasonPosition].equals("seasons")
              && urlPath[validUrlPathDaysPosition].equals("days")
              && urlPath[validUrlPathSkiersPosition].equals("skiers")
              && urlPath[validUrlPathResortNumberPosition]!= null
              && isNumeric(urlPath[validUrlPathResortNumberPosition])
              && urlPath[validUrlPathSeasonIDPosition]!= null
              && isNumeric(urlPath[validUrlPathSeasonIDPosition])
              && urlPath[validUrlPathDaysIDPosition]!= null
              && isNumeric(urlPath[validUrlPathDaysIDPosition])
              && urlPath[validUrlPathSkiersIDPosition]!= null
              && isNumeric(urlPath[validUrlPathSkiersIDPosition])
              && validDays.contains(urlPath[validUrlPathDaysIDPosition])
              && urlPath[validUrlPathSeasonIDPosition].equals(seasonID)
              && Integer.parseInt(urlPath[validUrlPathResortNumberPosition]) >= resortID_Min
              && Integer.parseInt(urlPath[validUrlPathResortNumberPosition]) <= resortID_Max
              && Integer.parseInt(urlPath[validUrlPathSkiersIDPosition]) >= skierID_Min
              && Integer.parseInt(urlPath[validUrlPathSkiersIDPosition]) <= skierID_Max;
    }
    return false;
  }


  private boolean isPostValid(LiftRideRecord liftRideRecord) {
    //  liftID - between 1 and 40
    //  time - between 1 and 360
    int time_Max = 360;
    int time_Min = 1;
    int liftID_Max = 40;
    int liftID_Min = 1;
    return liftRideRecord.getTime() >= time_Min
            && liftRideRecord.getTime() <= time_Max
            && liftRideRecord.getLiftID() >= liftID_Min
            && liftRideRecord.getLiftID() <= liftID_Max;
  }


  private  boolean isNumeric(String str) {
    // check number is Numeric or not
    try {
      Double.parseDouble(str);
      return true;
    } catch(NumberFormatException e){
      return false;
    }
  }

}

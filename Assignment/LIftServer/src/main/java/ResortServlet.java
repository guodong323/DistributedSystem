import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import model.LiftRideRecord;
import model.Message;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@WebServlet(name = "ResortServlet", value = "/resorts/*")
public class ResortServlet extends HttpServlet {

    private Gson gson = new Gson();
    private Properties properties = new Properties();
    private String rabbitMQName;
    private ConnectionFactory connectionFactory;
    private BlockingQueue<Channel> channelPool;
    private Connection connection;
    private int numberOfChannel;


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
        if (path.matches("/\\d+/seasons/\\d+/days/\\d+/skiers")) {
            String[] segments = path.split("/");
            String resortId = segments[1];
            String seasonId = segments[3];
            String dayId = segments[5];

            String jsonResponse = getResortSkiersDay(resortId, seasonId, dayId);

            response.setContentType("application/json");
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write(jsonResponse);
        } else {
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




    private static String getResortSkiersDay(String resortId, String seasonId, String dayId) {
        System.out.println("Querying for number of unique skiers at resort/season/day");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonResponse = mapper.createObjectNode();

        try {

            ScanRequest scanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .filterExpression("resortID = :v_resort")
                    .expressionAttributeValues(Map.of(
                            ":v_resort", AttributeValue.builder().s(resortId).build()))
                    .build();

            List<Map<String, AttributeValue>> items = dynamoDbClient.scan(scanRequest).items();
            System.out.println(items.size());

            List<Map<String, AttributeValue>> filteredItems = new ArrayList<>();


            for (Map<String, AttributeValue> item : items) {
                String[] parts = item.get("liftInfo").s().split(":");
                String seasonIdInRecord = parts[SEASON_ID_INDEX];
                String dayIdInRecord = parts[DAY_ID_INDEX];

                if (seasonIdInRecord.equals(seasonId) && dayIdInRecord.equals(dayId)) {
                    filteredItems.add(item);
                }
            }

            String key = String.format("number of unique skiers at %s/%s/%s", resortId, seasonId, dayId);
            jsonResponse.put(key, filteredItems.size());

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
}

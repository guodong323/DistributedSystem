
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDBHelper {


  public static String tableName = "LiftRideRecordTable";

  public static Region region = Region.US_WEST_2;

  public static DynamoDbClient dynamoDbClient;
  private static List<WriteRequest> putItemRequests = new ArrayList<>();

  private static int BATCH_INSERT_SIZE = 25;

  private static int RETRY_COUNT = 5;
  private static int BACK_OFF_TIME = 60;

  private static long successCount = 0;
  public static void init(){
    try{
      dynamoDbClient = DynamoDbClient.builder().region(region).build();
    } catch ( Exception e) {
      System.err.println("Fail to init Dynamo DB client" + e.getClass().getName() + ": " + e.getMessage());
    }
  }


  public static void createLiftRecordTable() {
    if (!checkTableExists(tableName)) {

      // Reason To Design in This way
      // We have to enable querys
      //      "For skier N, how many days have they skied this season?"
      //      "For skier N, what are the vertical totals for each ski day?" (calculate vertical as liftID*10)
      //      "For skier N, show me the lifts they rode on each ski day"
      //      "How many unique skiers visited resort X on day N?"
      // So set skier id as pri-key is good. and also we want to keep all info. so i concat all rest info as  liftInfo
      // Also, we want to get How many unique skiers visited resort X on day N?, that require another key, in Dynamo DB, global secondary key can perform the another key,
      CreateTableRequest request = CreateTableRequest.builder()
          .attributeDefinitions(AttributeDefinition.builder()
                  .attributeName("skierID")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("liftInfo")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("resortID")
                  .attributeType(ScalarAttributeType.S)
                  .build(),
              AttributeDefinition.builder()
                  .attributeName("dayID")
                  .attributeType(ScalarAttributeType.N)
                  .build()
              )
          .keySchema(KeySchemaElement.builder()
                  .attributeName("skierID")
                  .keyType(KeyType.HASH)
                  .build(),
              KeySchemaElement.builder()
                  .attributeName("liftInfo")
                  .keyType(KeyType.RANGE)
                  .build())
          .globalSecondaryIndexes(
              GlobalSecondaryIndex.builder()
                  .indexName("ResortAndDay")
                  .keySchema(
                      KeySchemaElement.builder()
                          .attributeName("resortID")
                          .keyType(KeyType.HASH)
                          .build(),
                      KeySchemaElement.builder()
                          .attributeName("dayID")
                          .keyType(KeyType.RANGE)
                          .build()
                  )
                  .projection(Projection.builder()
                      .projectionType(ProjectionType.INCLUDE)
                      .nonKeyAttributes("seasonID", "skierID")
                      .build())
                  .provisionedThroughput(
                      ProvisionedThroughput.builder()
                          .readCapacityUnits(5L)
                          .writeCapacityUnits(20L)
                          .build()
                  )
                  .build()
          )
          .provisionedThroughput(ProvisionedThroughput.builder()
              .readCapacityUnits(5L)
              .writeCapacityUnits(20L)
              .build())
          .tableName(tableName)
          .build();

      try {
        CreateTableResponse response = dynamoDbClient.createTable(request);
        System.out.println(tableName + " created successfully. Table status: " +
            response.tableDescription().tableStatus());
      } catch (DynamoDbException e) {
        System.err.println("Fail to create LiftRecordTable,  " + e.getMessage());
        System.exit(1);
      }
    }
  }


  public static void insert(Map<String, AttributeValue> item) {
    PutItemRequest request = PutItemRequest.builder()
        .tableName(tableName)
        .item(item)
        .build();
    try {
      dynamoDbClient.putItem(request);
      System.out.println("Item inserted successfully." + item.toString());
    } catch (Exception e) {
      System.err.println("Failed to insert item: " + item.toString());
      e.printStackTrace();
    }
  }

  public static void batchInsert(Map<String, AttributeValue> item) {

    putItemRequests.add(
        WriteRequest
            .builder()
            .putRequest(
                PutRequest
                    .builder()
                    .item(item)
                    .build())
            .build()
    );

    if (putItemRequests.size() >= BATCH_INSERT_SIZE){
      // batch put
      flush();
    }
  }

  public static synchronized void flush(){
    // for BatchWriteItemRequest
    Map<String, List<WriteRequest>> batchItems = new HashMap<>();
    batchItems.put(tableName, new ArrayList<>(putItemRequests));
    int retry = 0;
    BatchWriteItemResponse batchWriteItemResponse;

    do {
      // process batch
      BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
          .requestItems(batchItems)
          .build();
      batchWriteItemResponse = dynamoDbClient.batchWriteItem(batchWriteItemRequest);
      batchItems = batchWriteItemResponse.unprocessedItems();

      if (!batchItems.isEmpty()) {
        try {
          Thread.sleep((long) Math.pow(2, retry) * BACK_OFF_TIME);
        } catch (Exception e) {
          Thread.currentThread().interrupt();
          return;
        }
        retry++;
      }
    } while (retry < RETRY_COUNT && !batchItems.isEmpty());

    if (!batchWriteItemResponse.unprocessedItems().isEmpty()) {
      System.err.println("Failure items: " + batchWriteItemResponse.unprocessedItems());
      successCount += putItemRequests.size() - batchItems.values().stream().mapToInt(List::size).sum();
    } else {
      successCount += putItemRequests.size();
    }
    // only finished
//      System.out.println("Successful saved " + successCount + " item in to Dynamo DB");
    putItemRequests.clear();
  }

  private static boolean checkTableExists(String tableName) {
    try {
      DescribeTableRequest request = DescribeTableRequest.builder()
          .tableName(tableName)
          .build();
      dynamoDbClient.describeTable(request);
      return true; // Table exists
    } catch (ResourceNotFoundException e) {
      return false; // Table does not exist
    }
  }

  public static synchronized long getSuccessCount() {
    return successCount;
  }
}

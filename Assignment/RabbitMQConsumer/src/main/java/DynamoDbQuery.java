import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.endpoints.internal.Value.Str;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class DynamoDbQuery {
  public static String tableName = "LiftRideRecordTable";

  public static Region region = Region.US_WEST_2;

  public static DynamoDbClient dynamoDbClient;

  private static int SEASON_ID_INDEX = 0;
  private static int LIFT_ID_INDEX = 1;
  private static int DAY_ID_INDEX = 2;
  private static int TIME_ID_INDEX = 3;
  public static void init(){
    try{
      dynamoDbClient = DynamoDbClient.builder().region(region).build();
    } catch ( Exception e) {
      System.err.println("Fail to init Dynamo DB client" + e.getClass().getName() + ": " + e.getMessage());
    }
  }


  public static void main(String[] args) {

    init();
    String testSkier = "65061";
    String testSeason = "2024";
    String testResort = "10";
    String testDay = "1";
    query1(testSkier, testSeason);
    query2(testSkier);
    query3(testSkier);
    query4(testResort, testDay);
  }

  private static void query1(String skierId, String seasonId) {
    // "For skier N, how many days have they skied this season?"
    System.out.println(" For Query 1 ");
    try {
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("skierID = :v_id")
          .expressionAttributeValues(
              java.util.Map.of(":v_id", AttributeValue.builder().s(skierId).build()))
          .build();

      List<Map<String, AttributeValue>> items = dynamoDbClient.query(queryRequest).items();

      Set<String> uniqueDays = new HashSet<>();

      for (Map<String, AttributeValue> item : items) {
        String info = item.get("liftInfo").s();
        String[] parts = info.split(":");
        if (parts[SEASON_ID_INDEX].equals(seasonId)) {
          String dayId = parts[2];
          uniqueDays.add(dayId);
        }
      }

      String ans = String.format("For skier %s, %s days have skied in season %s?", skierId,
          uniqueDays.size(), seasonId);
      System.out.println(ans);
    } catch (Exception e) {
      System.err.println("has error" + e);
    }
  }

  private static void query2 (String skierId){
    // "For skier N, what are the vertical totals for each ski day?" (calculate vertical as liftID*10)"
    System.out.println(" For Query 2 ");
    int number = 10;
    try{
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("skierID = :v_id")
          .expressionAttributeValues(
              Map.of(":v_id", AttributeValue.builder().s(skierId).build()))
          .build();

      List<Map<String, AttributeValue>> items = dynamoDbClient.query(queryRequest).items();

      // Map to hold the vertical totals for each day
      Map<String, Integer> dayVerticalTotals = new HashMap<>();

      for (Map<String, AttributeValue> item : items) {
        String info = item.get("liftInfo").s();
        String[] parts = info.split(":");
        String seasonId = parts[SEASON_ID_INDEX];
        int liftID = Integer.parseInt(parts[LIFT_ID_INDEX]);
        String dayID = parts[DAY_ID_INDEX];
        String dayKey = seasonId + ":" + dayID; // Unique key for each day in a season

        // Calculate the vertical for this lift
        int vertical = liftID * number ;

        // Aggregate the vertical totals for each day
        dayVerticalTotals.merge(dayKey, vertical, Integer::sum);
      }

      // Print the vertical totals for each day
      dayVerticalTotals.forEach((day, totalVertical) ->
          System.out.println("For Skier: " + skierId + ", In Day: " + day  +  ", Total Vertical: " + totalVertical));
    } catch (Exception e) {
      System.err.println("has error" + e);
    }
  }


  private static void query3 (String skierId){
    // "For skier N, show me the lifts they rode on each ski day"
    System.out.println(" For Query 3 ");
    try{
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .keyConditionExpression("skierID = :v_id")
          .expressionAttributeValues(
               Map.of(":v_id", AttributeValue.builder().s(skierId).build()))
          .build();

      List<Map<String, AttributeValue>> items = dynamoDbClient.query(queryRequest).items();

      // Map to hold the lifts ridden for each day
      Map<String, Set<String>> dayLifts = new HashMap<>();

      for (Map<String, AttributeValue> item : items) {
        String info = item.get("liftInfo").s();
        String[] parts = info.split(":");

          String seasonId = parts[SEASON_ID_INDEX];
          String liftID = parts[LIFT_ID_INDEX];
          String dayID = parts[DAY_ID_INDEX];
          String dayKey = seasonId + ":" + dayID; // Unique key for each day in a season

          // Initialize the set of lifts if it doesn't already exist for the day
          dayLifts.putIfAbsent(dayKey, new HashSet<>());

          // Add the lift ID to the set of lifts for the day
          dayLifts.get(dayKey).add(liftID);
      }
      // Print the lifts ridden for each day
      dayLifts.forEach((day, lifts) -> {
        System.out.println("For Skier: " + skierId + ", In Day: " + day + ", Lifts Ridden: " + lifts);
      });
    } catch (Exception e) {
      System.err.println("has error" + e);
    }
  }


  private static void query4 (String resortID, String DayID){

    //"How many unique skiers visited resort X on day N?"
    System.out.println(" For Query 4 ");
    try{
      QueryRequest queryRequest = QueryRequest.builder()
          .tableName(tableName)
          .indexName("ResortDayIndex")
          .keyConditionExpression("resortID = :v_resort")
          .expressionAttributeValues(Map.of(
              ":v_resort", AttributeValue.builder().s(resortID).build()))
          .build();

      List<Map<String, AttributeValue>> items = dynamoDbClient.query(queryRequest).items();


      // Use a Set to hold unique skierIDs
      Set<String> uniqueSkiers = new HashSet<>();

      for (Map<String, AttributeValue> item : items) {
        String skierID = item.get("skierID").s();
        uniqueSkiers.add(skierID);
      }
      System.out.println("Number of unique skiers visited resort " + resortID + " on day " + DayID + ": " + uniqueSkiers.size() );

    } catch (Exception e) {
      System.err.println("has error" + e);
    }
  }
}

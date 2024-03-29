import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ConsumerThread implements Runnable {
  Gson gson = new Gson();
  private Connection connection;
  private String queueName;
  private int basicQos;

  public ConsumerThread(Connection connection, String queueName, int basicQos) {

    this.connection = connection;
    this.queueName = queueName;
    this.basicQos = basicQos;
  }

  @Override
  public void run() {

    try {
      Channel channel = connection.createChannel();
      channel.queueDeclare(queueName, false, false, false, null);
      channel.basicQos(basicQos);


      DeliverCallback deliverCallback = (consumerTag, delivery) -> {

        String message = new String(delivery.getBody(), "UTF-8");
        try {
          JsonObject liftRideRecord = gson.fromJson(message, JsonObject.class);
          int skierID = liftRideRecord.get("skierID").getAsInt();

          if ( Consumer.liftRecordsMap.containsKey(skierID) ) {
            Consumer.liftRecordsMap.get(skierID).add(liftRideRecord);
          } else {
            List<JsonObject> liftRideRecords = Collections.synchronizedList(new ArrayList<>());
            liftRideRecords.add(liftRideRecord);
            Consumer.liftRecordsMap.put(skierID, liftRideRecords);
          }


          // Since we have to make some query in dynamo DB, we have to change structure
//          Map<String, AttributeValue> item = new HashMap<>();
//          item.put("skierID", AttributeValue.builder().s(liftRideRecord.get("skierID").getAsString()).build());
//          item.put("resortID", AttributeValue.builder().s(liftRideRecord.get("resortID").getAsString()).build());
//          item.put("seasonID", AttributeValue.builder().s(liftRideRecord.get("seasonID").getAsString()).build());
//          item.put("day", AttributeValue.builder().s(liftRideRecord.get("dayID").getAsString()).build());
//          item.put("liftID", AttributeValue.builder().s(liftRideRecord.get("liftID").getAsString()).build());
//          item.put("time", AttributeValue.builder().s(liftRideRecord.get("time").getAsString()).build());

          Map<String, AttributeValue> item = new HashMap<>();
          // primary key
          item.put("skierID", AttributeValue.builder().s(liftRideRecord.get("skierID").getAsString()).build());

          // Secondary key
          String time = liftRideRecord.get("time").getAsString();
          String seasonID = liftRideRecord.get("seasonID").getAsString();
          String day = liftRideRecord.get("dayID").getAsString();
          String liftID = liftRideRecord.get("liftID").getAsString();
          String rangeKey = seasonID + ":" + liftID + ":" + day + ":" + time;
          item.put("liftInfo", AttributeValue.builder().s(rangeKey).build());

          //
          item.put("resortID", AttributeValue.builder().s(liftRideRecord.get("resortID").getAsString()).build());
          // DynamoDBHelper.insert(item);
          DynamoDBHelper.batchInsert(item);

//          System.out.println("Successful consume object: " + liftRideRecord + ", Thread Id is: " + Thread.currentThread().getId());

        } catch ( Exception e) {
          String error_message = String.format("Fail to consume Object %s", message);
          System.out.println(error_message + e);
        }
      };
      channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

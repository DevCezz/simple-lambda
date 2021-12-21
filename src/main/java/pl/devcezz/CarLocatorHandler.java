package pl.devcezz;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class CarLocatorHandler implements RequestHandler<SQSEvent, String> {

    private static final String TABLE_NAME = "CarLocatorTable";

    private static final SQSEventProcessor processor = new SQSEventProcessor();
    private static final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public String handleRequest(final SQSEvent event, final Context context) {
        Table table = dynamoDB.getTable(TABLE_NAME);
        LambdaLogger logger = context.getLogger();

        event.getRecords().forEach(message -> {
            String json = processor.processMessageToJson(message);

            logger.log("Reading data: " + json);
            CarLocation carLocation = gson.fromJson(json, CarLocation.class);

            Item item = new Item()
                    .withPrimaryKey("id", carLocation.id)
                    .withNumber("latitude", carLocation.latitude)
                    .withNumber("longitude", carLocation.longitude);
            table.putItem(item);

            logger.log("Data successfully added to database");
        });

        return "SUCCESS";
    }
}

class CarLocation {
    long id;
    double latitude;
    double longitude;
}
package pl.devcezz;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

class SQSEventProcessor {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public String processMessageToJson(SQSEvent.SQSMessage message) {
        SQSMessageBody sqsMessageBody = gson.fromJson(message.getBody(), SQSMessageBody.class);
        return purgeMessage(sqsMessageBody.Message);
    }

    private String purgeMessage(final String message) {
        return message
                .replace("\\", "")
                .replace("\"{", "{")
                .replace("}\"", "}");
    }

    private static class SQSMessageBody {
        String Type;
        String MessageId;
        String TopicArn;
        String Message;
        String Timestamp;
        String SignatureVersion;
        String Signature;
        String SigningCertURL;
        String UnsubscribeURL;
    }
}

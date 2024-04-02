package net.bondarik;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import net.bondarik.model.MessageWithDate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class TaskProducer
{
    private final static String TASK_QUEUE_NAME = "tasks";
    private final static boolean isDurable = true;
    private static final Scanner SCANNER = new Scanner(System.in);


    public static void main( String[] args ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, isDurable, false, false, null);

            while (true) {
                publish(channel, SCANNER.nextLine());
            }
        }
    }

    private static void publish(Channel channel, String message) throws Exception {
        MessageWithDate messageWithDate = new MessageWithDate(message);
        channel.basicPublish("", TASK_QUEUE_NAME,
                             MessageProperties.PERSISTENT_TEXT_PLAIN,
                             serialize(messageWithDate));
        System.out.println(" [x] Sent '" + message + "'");
    }

    private static byte[] serialize(MessageWithDate messageWithDate) {
        ByteArrayOutputStream biteArrayStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(biteArrayStream);
            outputStream.writeObject(messageWithDate);
            outputStream.flush();
            return biteArrayStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

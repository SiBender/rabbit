package net.bondarik;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class Producer
{
    private final static String QUEUE_NAME = "hello";
    private static final Scanner SCANNER = new Scanner(System.in);
    public static void main( String[] args ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            while (true) {
                publish(channel, SCANNER.nextLine());
            }
        }
    }

    private static void publish(Channel channel, String message) throws Exception {
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }
}

package net.bondarik;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class DirectExchangeWithBindingProducer
{
    private final static String EXCHANGE_NAME = "direct_logs";
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final String[] SEVERITIES = new String[]{"red", "green", "yellow"};
    private static int counter = 0;


    public static void main( String[] args ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            while (true) {
                publish(channel, SCANNER.nextLine());
            }
        }
    }

    private static void publish(Channel channel, String message) throws Exception {
        counter++;
        int index = counter % SEVERITIES.length;
        channel.basicPublish(EXCHANGE_NAME, SEVERITIES[index], null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }
}

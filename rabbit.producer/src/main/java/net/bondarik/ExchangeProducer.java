package net.bondarik;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.util.Scanner;

/**
 * Hello world!
 *
 */
public class ExchangeProducer
{
    private final static String EXCHANGE_NAME = "logs";
    private final static boolean isDurable = true;
    private static final Scanner SCANNER = new Scanner(System.in);
    public static void main( String[] args ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

            while (true) {
                publish(channel, SCANNER.nextLine());
            }
        }
    }

    private static void publish(Channel channel, String message) throws Exception {
        channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
        System.out.println(" [x] Sent '" + message + "'");
    }
}

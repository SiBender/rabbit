package net.bondarik;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Scanner;

public class DirectExchangeConsumer {
    private final static String EXCHANGE_NAME = "direct_logs";
    private static final Scanner SCANNER = new Scanner(System.in);
    private static final String[] SEVERITIES = new String[]{"red", "green", "yellow"};


    public static void main( String[] args ) throws Exception {
        Integer severityId = Integer.valueOf(SCANNER.nextLine()) % SEVERITIES.length;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, SEVERITIES[severityId]);

        channel.basicConsume(queueName, true, getDeliverCallback(channel), consumerTag -> { });
    }

    private static DeliverCallback getDeliverCallback(Channel channel) {
        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received '" + message + "'");
            processTask(message);
            System.out.println(" [x] Done");
        };
    }

    private static void processTask(String taskMessage) {
        long start = System.currentTimeMillis();
        try {
            Thread.sleep(taskMessage.length() * 10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            long finish = System.currentTimeMillis();
            System.out.println("Message '" + taskMessage + "' processed in " + (finish - start) + " ms");
        }
    }
}

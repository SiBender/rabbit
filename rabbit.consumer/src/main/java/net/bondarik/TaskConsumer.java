package net.bondarik;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * Hello world!
 *
 */
public class TaskConsumer
{
    private final static String TASK_QUEUE_NAME = "tasks";
    private static final boolean isDurable = true;

    private static int counter = 0;


    public static void main( String[] args ) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("producer");
        factory.setPassword("producer");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, isDurable, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");


        boolean autoAck = false; // acknowledgment is covered below
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, getDeliverCallback(channel), consumerTag -> { });
    }

    private static DeliverCallback getDeliverCallback(Channel channel) {
        return (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");

            System.out.println(" [x] Received '" + message + "'");
            processTask(message);
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };
    }

    private static void processTask(String taskMessage) {
        counter++;
        /*if (counter % 3 == 2) {
            throw new RuntimeException("Ой, всё!");
        }*/

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

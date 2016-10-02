using RabbitMQ.Client;
using RabbitMQ.Examples;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PaymentCardConsumer
{
    class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;

        private const string PaymentTopicExchange = "payment_topic_exchange";
        private const string CardPaymentTopicQueueName = "card_payment_topic_queue";

        static void Main(string[] args)
        {
            _factory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };
            using (var _connection = _factory.CreateConnection())
            {
                using (var channel = _connection.CreateModel())
                {
                    channel.ExchangeDeclare(PaymentTopicExchange, "topic");
                    channel.QueueDeclare(CardPaymentTopicQueueName, true, false, false, null);
                    channel.QueueBind(CardPaymentTopicQueueName, PaymentTopicExchange, "payment.card");
                    channel.BasicQos(0, 1, false);

                    var consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume(CardPaymentTopicQueueName, false, consumer);

                    while (true)
                    {
                        var ea = consumer.Queue.Dequeue();
                        var message = (Payment)ea.Body.DeSerialize(typeof(Payment));
                        var routingKey = ea.RoutingKey;
                        channel.BasicAck(ea.DeliveryTag, false);
                        Console.WriteLine("--- Payment - Routing Key <{0}> : {1} : {2}", routingKey, message.CardNumber, message.AmountToPay);
                    }
                }
            }
        }
    }
}

using RabbitMQ.Client;
using RabbitMQ.Examples;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PurchaseOrderConsumer
{
    class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;

        private const string PaymentTopicExchange = "payment_topic_exchange";
        private const string PurchaseOrderTopicQueueName = "purchase_order_topic_queue";

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
                    channel.QueueDeclare(PurchaseOrderTopicQueueName, true, false, false, null);
                    channel.QueueBind(PurchaseOrderTopicQueueName, PaymentTopicExchange, "payment.purchaseorder");
                    channel.BasicQos(0, 1, false);

                    var consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume(PurchaseOrderTopicQueueName, false, consumer);

                    while (true)
                    {
                        var ea = consumer.Queue.Dequeue();
                        var message = (PurchaseOrder)ea.Body.DeSerialize(typeof(PurchaseOrder));
                        var routingKey = ea.RoutingKey;
                        channel.BasicAck(ea.DeliveryTag, false);
                        Console.WriteLine("--- Purchase Order - Routing Key <{0}> : {1}, ${2}, {3}, {4}", routingKey, message.CompanyName, message.AmountToPay, message.PaymentDayTerms, message.PoNumber);
                    }
                }
            }
        }
    }
}

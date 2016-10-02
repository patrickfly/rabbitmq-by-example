using RabbitMQ.Client;
using RabbitMQ.Examples;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AccountsAuditConsumer
{
    class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;

        private const string PaymentTopicExchange = "payment_topic_exchange";
        private const string AllTopicQueueName = "all_topic_queue";

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
                    channel.QueueDeclare(AllTopicQueueName, true, false, false, null);
                    channel.QueueBind(AllTopicQueueName, PaymentTopicExchange, "payment.*");
                    channel.BasicQos(0, 1, false);

                    var consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume(AllTopicQueueName, false, consumer);

                    while (true)
                    {
                        var ea = consumer.Queue.Dequeue();
                        var routingKey = ea.RoutingKey;
                        switch (routingKey)
                        {
                            case "payment.card":
                                var paymentMessage = (Payment)ea.Body.DeSerialize(typeof(Payment));
                                Console.WriteLine("--- Payment - Routing Key <{0}> : {1} : {2}", routingKey, paymentMessage.CardNumber, paymentMessage.AmountToPay);
                                break;
                            case "payment.purchaseorder":
                                var purchaseOrderMessage = (PurchaseOrder)ea.Body.DeSerialize(typeof(PurchaseOrder));
                                Console.WriteLine("--- Purchase Order - Routing Key <{0}> : {1}, ${2}, {3}, {4}", routingKey, purchaseOrderMessage.CompanyName, purchaseOrderMessage.AmountToPay, purchaseOrderMessage.PaymentDayTerms, purchaseOrderMessage.PoNumber);
                                break;
                        }
                        
                        channel.BasicAck(ea.DeliveryTag, false);
                        
                    }
                }
            }
        }
    }
}

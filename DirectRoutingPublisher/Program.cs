using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Examples
{
    class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;

        private const string ExchangeName = "DirectRouting_Exchange";
        private const string CardPaymentQueueName = "CardPaymentDirectRouting_Queue";
        private const string PurchaseOrderQueueName = "PurchaseOrderDirectRouting_Queue";

        static void Main(string[] args)
        {
            var payments = new List<Payment>();
            for (var i = 0; i < 10; i++)
            {
                payments.Add(new Payment
                {
                    AmountToPay = 25.0m + i,
                    CardNumber = "1234567890",
                    Name = "my name " + i.ToString()
                });
            }

            var purchases = new List<PurchaseOrder>();
            for (var i = 0; i < 10; i++)
            {
                purchases.Add(new PurchaseOrder
                {
                    AmountToPay = 25.0m + i,
                    CompanyName = "Company " + i,
                    PaymentDayTerms = i,
                    PoNumber = i.ToString()
                });
            }

            CreateConnection();

            payments.ForEach((payment) =>
            {
                SendPayment(payment);
            });

            purchases.ForEach((purchase) =>
            {
                SendPurchaseOrder(purchase);
            });
        }

        private static void SendPayment(Payment message)
        {
           SendMessage(message.Serialize(), "CardPayment");
            Console.WriteLine(" Payment Sent {0}, ${1}", message.CardNumber, message.AmountToPay);
        }

        private static void SendPurchaseOrder(PurchaseOrder message)
        {
            SendMessage(message.Serialize(), "PurchaseOrder");
            Console.WriteLine(" Purchase Order Sent {0}, ${1}, {2}, {3}", message.CompanyName, message.AmountToPay, message.PaymentDayTerms, message.PoNumber);
        }

        private static void SendMessage(byte[] msg, string routingKey)
        {
            _model.BasicPublish(ExchangeName, routingKey, null, msg);
        }

        private static void CreateConnection()
        {
            _factory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };
            _connection = _factory.CreateConnection();
            _model = _connection.CreateModel();
            _model.ExchangeDeclare(ExchangeName, "direct");
            _model.QueueDeclare(CardPaymentQueueName, true, false, false, null);
            _model.QueueDeclare(PurchaseOrderQueueName, true, false, false, null);
            _model.QueueBind(CardPaymentQueueName, ExchangeName, "CardPayment");
            _model.QueueBind(PurchaseOrderQueueName, ExchangeName, "PurchaseOrder");
        }

    }
}

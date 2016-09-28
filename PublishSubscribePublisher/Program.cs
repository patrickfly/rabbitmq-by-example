using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Examples
{
    public class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;

        private const string ExchangeName = "Publish_Subscribe_Exchange";

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

            CreateConnection();

            payments.ForEach((payment) =>
            {
                SendMessage(payment);
            });

            Console.ReadLine();
        }

        private static void SendMessage(Payment message)
        {
            _model.BasicPublish(ExchangeName, "", null, message.Serialize());
            Console.WriteLine(" Payment Sent {0}, ${1}", message.CardNumber, message.AmountToPay);
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
            _model.ExchangeDeclare(ExchangeName, "fanout", false);
        }
    }
}

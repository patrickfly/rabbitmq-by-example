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
        private const string QueueName = "StandardQueue_ExampleQueue";

        public static void Main(string[] args)
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

            CreateQueue();
            payments.ForEach((payment) =>
            {
                SendMessage(payment);
            });
            Receive();
            Console.ReadLine();
        }

        private static void CreateQueue()
        {
            _factory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };
            _connection = _factory.CreateConnection();
            _model = _connection.CreateModel();
            _model.QueueDeclare(QueueName, true, false, false, null);
        }

        private static void SendMessage(Payment message)
        {
            _model.BasicPublish("", QueueName, null, message.Serialize());
            Console.WriteLine(" [x] Payment Message Sent: {0} : {1} : {2}", message.CardNumber, message.AmountToPay, message.Name);
        }

        public static void Receive()
        {
            var consumer = new QueueingBasicConsumer(_model);
            var msgCount = GetMessageCount(_model, QueueName);

            _model.BasicConsume(QueueName, true, consumer);

            var count = 0;
            while (count < msgCount)
            {
                var message = (Payment)consumer.Queue.Dequeue().Body.DeSerialize(typeof(Payment));
                Console.WriteLine("----- Received {0} : {1} : {2}", message.CardNumber, message.AmountToPay, message.Name);
                count++;
            }
        }

        private static uint GetMessageCount(IModel channel, string queueName)
        {
            var results = channel.QueueDeclare(queueName, true, false, false, null);
            return results.MessageCount;
        }
    }
}

using RabbitMQ.Client;
using RabbitMQ.Examples;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DirectPaymentCardConsumer
{
    class Program
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static Random _rnd;

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
                    channel.QueueDeclare("card_payment_rpc_queue", true, false, false, null);
                    channel.QueueDeclare("card_payment_rpc_reply_queue", true, false, false, null);
                    channel.BasicQos(0, 1, false);

                    var consumer = new QueueingBasicConsumer(channel);
                    channel.BasicConsume("card_payment_rpc_queue", false, consumer);
                    _rnd = new Random();

                    while (true)
                    {
                        var ea = consumer.Queue.Dequeue();
                        var props = ea.BasicProperties;
                        var replyProps = channel.CreateBasicProperties();
                        replyProps.CorrelationId = props.CorrelationId;
                        string response = null;
                        Console.WriteLine("----------------------------------------------------");
                        try
                        {
                            var message = (Payment)ea.Body.DeSerialize(typeof(Payment));
                            response = _rnd.Next(1000, 100000000).ToString(CultureInfo.InvariantCulture);
                            Console.WriteLine("--- Payment - Auth Code <{0}> : {1} : {2}", response, message.CardNumber, message.AmountToPay);
                            Console.WriteLine("Correlation ID: {0}", props.CorrelationId);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(" ERROR: {0}", e.Message);
                            response = null;
                        }
                        finally
                        {
                            if (response != null)
                            {
                                channel.BasicPublish("", props.ReplyTo, replyProps, Encoding.UTF8.GetBytes(response));
                            }
                            channel.BasicAck(ea.DeliveryTag, false);
                        }
                        Console.WriteLine("----------------------------------------------------");
                        Console.WriteLine("");
                    }
                }
            }
        }
    }
}

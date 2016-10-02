using RabbitMQ.Client;
using RabbitMQ.Examples;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Web;
using System.Web.Http;

namespace PaymentsAPI.Controllers
{
    public class DirectCardPaymentController : ApiController
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;
        private static string _replyQueueName;
        private static QueueingBasicConsumer _consumer;

        private const string PaymentTopicExchange = "payment_topic_exchange";
        private const string CardPaymentTopicQueueName = "card_payment_topic_queue";
        private const string PurchaseOrderTopicQueueName = "purchase_order_topic_queue";
        private const string AllTopicQueueName = "all_topic_queue";

        [HttpPost]
        public IHttpActionResult MakePayment([FromBody] Payment payment)
        {
            try
            {
                _factory = new ConnectionFactory
                {
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest"
                };
                _connection = _factory.CreateConnection();
                _model = _connection.CreateModel();
                _replyQueueName = _model.QueueDeclare("card_payment_rpc_reply_queue", true, false, false, null);
                _model.QueueDeclare("card_payment_rpc_queue", true, false, false, null);
                _consumer = new QueueingBasicConsumer(_model);
                _model.BasicConsume(_replyQueueName, true, _consumer);

                var props = _model.CreateBasicProperties();
                props.ReplyTo = _replyQueueName;
                props.CorrelationId = Guid.NewGuid().ToString();
                _model.BasicPublish("", "card_payment_rpc_queue", props, payment.Serialize());

                while (true)
                {
                    var ea = _consumer.Queue.Dequeue();
                    if (ea.BasicProperties.CorrelationId != props.CorrelationId)
                    {
                        continue;
                    }
                    return Ok(Encoding.UTF8.GetString(ea.Body));
                }
            }
            catch (Exception)
            {
                return StatusCode(HttpStatusCode.BadRequest);
            }
        }
    }
}
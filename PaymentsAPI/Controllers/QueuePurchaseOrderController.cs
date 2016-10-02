using RabbitMQ.Client;
using RabbitMQ.Examples;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Http;

namespace PaymentsAPI.Controllers
{
    public class QueuePurchaseOrderController : ApiController
    {
        private static ConnectionFactory _factory;
        private static IConnection _connection;
        private static IModel _model;

        private const string PaymentTopicExchange = "payment_topic_exchange";
        private const string CardPaymentTopicQueueName = "card_payment_topic_queue";
        private const string PurchaseOrderTopicQueueName = "purchase_order_topic_queue";
        private const string AllTopicQueueName = "all_topic_queue";

        [HttpPost]
        public IHttpActionResult MakePayment([FromBody] PurchaseOrder purchaseOrder)
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
                _model.ExchangeDeclare(PaymentTopicExchange, "topic");
                _model.QueueDeclare(CardPaymentTopicQueueName, true, false, false, null);
                _model.QueueDeclare(PurchaseOrderTopicQueueName, true, false, false, null);
                _model.QueueDeclare(AllTopicQueueName, true, false, false, null);
                _model.QueueBind(CardPaymentTopicQueueName, PaymentTopicExchange, "payment.card");
                _model.QueueBind(PurchaseOrderTopicQueueName, PaymentTopicExchange, "payment.purchaseorder");
                _model.QueueBind(AllTopicQueueName, PaymentTopicExchange, "payment.*");

                _model.BasicPublish(PaymentTopicExchange, "payment.purchaseorder", null, purchaseOrder.Serialize());
            }
            catch (Exception)
            {
                return StatusCode(HttpStatusCode.BadRequest);
            }
            return Ok(purchaseOrder);
        }
    }
}
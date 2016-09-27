using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Examples
{
    [Serializable]
    public class Payment
    {
        public decimal AmountToPay;
        public string CardNumber;
        public string Name;
    }
}

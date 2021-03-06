﻿using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Examples
{
    public static class ObjectSerialize
    {
        public static byte[] Serialize(this object obj)
        {
            if (obj == null)
            {
                return null;
            }
            var json = JsonConvert.SerializeObject(obj);
            return Encoding.ASCII.GetBytes(json);
        }

        public static object DeSerialize(this byte[] arrBytes, Type type)
        {
            return JsonConvert.DeserializeObject(DeSerializeText(arrBytes), type);
        }

        public static string DeSerializeText(this byte[] arrBytes)
        {
            return Encoding.Default.GetString(arrBytes);
        }
    }
}

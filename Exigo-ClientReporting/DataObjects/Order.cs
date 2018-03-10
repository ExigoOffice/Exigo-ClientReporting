using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exigo.ClientReporting
{
    public class Order
    {
        public int OrderID { get; set; }
        public int CustomerID { get; set; }
        public int PriceTypeID { get; set; }
        public int WarehouseID { get; set; }
        public int OrderTypeID { get; set; }
        public int OrderStatusID { get; set; }
        public string CurrencyCode { get; set; }
        public DateTime OrderDate { get; set; }
        
        public decimal SubTotal { get; set; }
        public decimal CV { get; set; }
        public decimal BV { get; set; }

        public Customer Customer { get; set; }


    }
}

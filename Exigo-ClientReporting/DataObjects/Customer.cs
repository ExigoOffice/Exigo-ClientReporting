using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exigo.ClientReporting
{
    public class Customer
    {

        public int CustomerID { get; set; }
        public int CustomerTypeID { get; set; }
        public int CustomerStatusID { get; set; }
        public int RankID { get; set; }
        public DateTime EntryDate { get; set; }
        public string CountryCode { get; set; }
        public string CurrencyCode { get; set; }
        public DateTime StartDate { get; set; }

        public Node UnilevelNode { get; set; }
        public Node EnrollerNode { get; set; }
        public Node BinaryNode { get; set; }

        public bool IsInUnilevelTree { get { return (UnilevelNode == null); } }
        public bool IsInEnrollerTree { get { return (EnrollerNode == null); } }
        public bool IsInBinaryTree { get { return (BinaryNode == null); } }

        public bool IsActive { get; internal set; }
        public Volume Volume { get; internal set; }
}
}

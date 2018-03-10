using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exigo.ClientReporting
{
    public class Volume 
    {
        public Volume(int bucketCount)
        {
            _Volumes = new Dictionary<int, decimal>(bucketCount);
        }
        private Dictionary<int, decimal> _Volumes;

        public decimal this[int volumeID]
        {
            internal set
            {
                _Volumes[volumeID] = value;
            }
            get
            {
                return _Volumes[volumeID];
            }
        }


        public int CustomerID { get; set; }
        public int RankID { get; set; }
        public int PaidRankID { get; set; }
    }
}

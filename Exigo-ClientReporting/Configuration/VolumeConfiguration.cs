using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exigo.ClientReporting
{
    public class VolumeField
    {
        public int VolumeID { get; set; }
        public string FieldName { get; set; }
    }

    public class VolumeConfiguration
    {
        public VolumeConfiguration()
        {
            VolumeFields = new List<VolumeField>();
        }

        /// <summary>
        /// List of Volume Fields
        /// </summary>
        public  List<VolumeField> VolumeFields { get; set; }  
    }
}
